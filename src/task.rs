use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use chrono::{DateTime, Utc};
use diesel::{
    JoinOnDsl, NullableExpressionMethods, OptionalExtension, QueryDsl,
    sql_types::{Array, BigInt, VarChar},
};
use diesel_async::{AsyncPgConnection, RunQueryDsl, scoped_futures::ScopedFutureExt};
use rusty_pool::ThreadPool;
use s3::Bucket;
use tokio::{runtime::Handle, sync::Mutex, task::JoinHandle};

use lazy_static::lazy_static;
use uuid::Uuid;

use crate::broker::{get_broker_access_quota, get_broker_quota_used_by_user};
use crate::model::{
    NewReconcileBrokerQuotaUsageTask, ReconcileBrokerQuotaUsageTask, get_system_user,
};
use crate::post::delete::execute_delete_posts;
use crate::schema::{apply_auto_tags_task, post, reconcile_broker_quota_usage_task, s3_object};
use crate::tag::auto_matching::run_apply_auto_tags_task;
use crate::{
    acquire_db_connection,
    data::{
        create_bucket,
        encode::{self, SUBMITTED_HLS_TRANSCODINGS, VIDEO_TRANSCODE_SEMAPHORE},
    },
    diesel::{BoolExpressionMethods, ExpressionMethods},
    error::Error,
    model::{ApplyAutoTagsTask, Broker, DeferredS3ObjectDeletion, S3Object, User},
    retry_on_constraint_violation, retry_on_serialization_failure, run_serializable_transaction,
    schema::{
        broker, deferred_s3_object_deletion, email_confirmation_token, one_time_password,
        refresh_token, registered_user,
    },
};

lazy_static! {
    pub static ref TASK_POOL: ThreadPool = {
        let task_pool_worker_count = std::env::var("FILEBROKER_TASK_POOL_WORKER_COUNT")
            .map(|s| {
                s.parse::<usize>()
                    .expect("FILEBROKER_TASK_POOL_WORKER_COUNT invalid")
            })
            .unwrap_or(4);
        rusty_pool::Builder::new()
            .core_size(task_pool_worker_count)
            .max_size(task_pool_worker_count)
            .name(String::from("task_pool"))
            .build()
    };
    pub static ref RUNNING_TASK_IDS: flurry::HashSet<&'static str> = flurry::HashSet::new();
    pub static ref DISABLE_GENERATE_MISSING_HLS_STREAMS: bool =
        std::env::var("FILEBROKER_DISABLE_GENERATE_MISSING_HLS_STREAMS")
            .map(|s| s
                .parse::<bool>()
                .expect("FILEBROKER_DISABLE_GENERATE_MISSING_HLS_STREAMS is invalid"))
            .unwrap_or(false);
    pub static ref DISABLE_GENERATE_MISSING_THUMBNAILS: bool =
        std::env::var("FILEBROKER_DISABLE_GENERATE_MISSING_THUMBNAILS")
            .map(|s| s
                .parse::<bool>()
                .expect("FILEBROKER_DISABLE_GENERATE_MISSING_THUMBNAILS is invalid"))
            .unwrap_or(false);
    pub static ref DISABLE_LOAD_MISSING_METADATA: bool =
        std::env::var("FILEBROKER_DISABLE_LOAD_MISSING_METADATA")
            .map(|s| s
                .parse::<bool>()
                .expect("FILEBROKER_DISABLE_LOAD_MISSING_METADATA is invalid"))
            .unwrap_or(false);
    pub static ref IS_SHUTDOWN: AtomicBool = AtomicBool::new(false);
    pub static ref RECONCILE_QUOTA_GRACE_PERIOD: pg_interval::Interval =
        std::env::var("FILEBROKER_RECONCILE_QUOTA_GRACE_PERIOD")
            .map(|s| pg_interval::Interval::from_iso(&s)
                .expect("FILEBROKER_RECONCILE_QUOTA_GRACE_PERIOD is invalid"))
            .unwrap_or(pg_interval::Interval::new(0, 30, 0));
}

pub fn submit_task(
    task_id: &'static str,
    task: impl Fn(Handle) -> Result<(), Error> + Send + 'static,
) {
    if AtomicBool::load(&IS_SHUTDOWN, Ordering::Relaxed) {
        log::warn!("Skipping task {task_id} because the task pool is shutting down");
        return;
    }
    let tokio_handle = Handle::current();
    ThreadPool::execute(&TASK_POOL, move || {
        // check again when running in pool
        if AtomicBool::load(&IS_SHUTDOWN, Ordering::Relaxed) {
            log::warn!("Skipping task {task_id} because the task pool is shutting down");
            return;
        }
        let running_task_ids = RUNNING_TASK_IDS.pin();
        // only run task if not already running
        if running_task_ids.insert(task_id) {
            let _sentinel = TaskSentinel {
                task_id,
                running_task_ids,
            };

            log::info!("Starting task {task_id}");
            let now = std::time::Instant::now();
            if let Err(e) = task(tokio_handle) {
                log::error!("Error executing task {task_id}: {e}");
            }
            log::info!("Finished task {task_id} after {:?}", now.elapsed());
        } else {
            log::warn!("Skipping task {task_id} because it is already running")
        }
    })
}

pub fn shutdown_join() {
    IS_SHUTDOWN.store(true, Ordering::Relaxed);
    TASK_POOL.join();
}

struct TaskSentinel<'a> {
    task_id: &'static str,
    running_task_ids: flurry::HashSetRef<'a, &'static str>,
}

impl Drop for TaskSentinel<'_> {
    fn drop(&mut self) {
        self.running_task_ids.remove(self.task_id);
    }
}

#[allow(clippy::await_holding_lock)] // allow SUBMITTED_HLS_TRANSCODINGS to be held while waiting for query to load relevant objects
pub fn generate_missing_hls_streams(tokio_handle: Handle) -> Result<(), Error> {
    if *DISABLE_GENERATE_MISSING_HLS_STREAMS {
        log::info!("generate_missing_hls_streams disabled");
        return Ok(());
    }
    if !encode::is_hls_supported_on_current_platform() {
        log::warn!(
            "Skipping generate_missing_hls_streams because it is unsupported on the current platform"
        );
        return Ok(());
    }
    tokio_handle.block_on(async {
        log::debug!("Waiting to acquire permit to start HLS transcoding");
        let _semaphore = VIDEO_TRANSCODE_SEMAPHORE.acquire().await
            .map_err(|_| Error::CancellationError)?;

        loop {
            let mut connection = acquire_db_connection().await?;

            let submitted_hls_transcodings_guard = SUBMITTED_HLS_TRANSCODINGS.lock();
            let submitted_hls_transcodings = submitted_hls_transcodings_guard.iter().collect::<Vec<_>>();
            let relevant_objects = run_serializable_transaction(&mut connection, |connection| async move {
                diesel::sql_query("
                WITH relevant_s3objects AS(
                    SELECT * FROM s3_object AS obj
                    WHERE NOT hls_disabled
                    AND NOT(obj.object_key = ANY($1))
                    AND hls_master_playlist IS NULL
                    AND LOWER(mime_type) LIKE 'video/%'
                    AND hls_locked_at IS NULL
                    AND EXISTS(SELECT * FROM post WHERE s3_object = obj.object_key)
                    AND EXISTS(SELECT * FROM broker WHERE pk = obj.fk_broker AND hls_enabled)
                    AND NOT EXISTS(SELECT * FROM s3_object WHERE thumbnail_object_key = obj.object_key)
                    AND NOT EXISTS(SELECT * FROM hls_stream WHERE stream_playlist = obj.object_key OR stream_file = obj.object_key OR master_playlist = obj.object_key)
                    AND (hls_fail_count IS NULL OR hls_fail_count < 3)
                    ORDER BY hls_fail_count ASC NULLS FIRST, creation_timestamp ASC
                    LIMIT 25
                )
                UPDATE s3_object SET hls_locked_at = NOW() WHERE hls_locked_at IS NULL AND object_key IN(SELECT object_key FROM relevant_s3objects) RETURNING *;
            ")
            .bind::<Array<VarChar>, _>(submitted_hls_transcodings)
            .load::<S3Object>(connection)
            .await
            .map_err(retry_on_serialization_failure)
            }.scope_boxed()).await?;
            drop(submitted_hls_transcodings_guard);
            drop(connection);

            if relevant_objects.is_empty() {
                break;
            }

            let _sentinel = LockedObjectsTaskSentinel::new(
                "hls_locked_at",
                relevant_objects
                    .iter()
                    .map(|o| o.object_key.clone())
                    .collect::<Vec<String>>(),
                &tokio_handle,
            ).await;

            log::info!(
                "Found {} objects with missing HLS playlists",
                relevant_objects.len()
            );
            for object in relevant_objects {
                if AtomicBool::load(&IS_SHUTDOWN, Ordering::Relaxed) {
                    log::warn!("Stopping task generate_missing_hls_streams because the task pool is shutting down");
                    return Ok(());
                }
                let mut connection = acquire_db_connection().await?;

                let (bucket, broker, user) =
                    match load_object_relations(object.fk_broker, object.fk_uploader, &mut connection).await {
                        Ok(res) => res,
                        Err(e) => {
                            log::error!("Failed to load data for {}: {}", &object.object_key, e);
                            continue;
                        }
                    };
                drop(connection);

                let file_id = match Path::new(&object.object_key)
                    .file_stem()
                    .map(|o| o.to_string_lossy().to_string())
                {
                    Some(file_id) => match Uuid::parse_str(&file_id) {
                        Ok(uuid) => uuid,
                        Err(e) => {
                            log::error!(
                                "Failed to get file stem for object key '{}': {}",
                                &object.object_key,
                                e
                            );
                            continue;
                        }
                    },
                    None => {
                        log::error!(
                            "Failed to get file stem for object key '{}'",
                            &object.object_key
                        );
                        continue;
                    }
                };

                if let Err(e) = encode::generate_hls_playlist(
                    bucket,
                    object.object_key.clone(),
                    file_id,
                    broker,
                    user,
                    true,
                ).await {
                    log::error!(
                        "Failed HLS transcoding of object {}: {}",
                        &object.object_key,
                        e
                    );
                    if AtomicBool::load(&IS_SHUTDOWN, Ordering::Relaxed) {
                        log::warn!("Stopping task generate_missing_hls_streams because the task pool is shutting down");
                        return Ok(());
                    }
                    if let Ok(mut connection) = acquire_db_connection().await
                        && let Err(e) = diesel::sql_query("UPDATE s3_object SET hls_fail_count = coalesce(hls_fail_count, 0) + 1 WHERE object_key = $1")
                            .bind::<VarChar, _>(&object.object_key)
                            .execute(&mut connection).await {
                                log::error!("Failed to increment hls_fail_count: {e}");
                    }
                } else {
                    log::info!(
                        "Generated missing HLS stream for object {}",
                        &object.object_key
                    );
                }
            }
        }

        Ok(())
    })
}

pub fn generate_missing_thumbnails(tokio_handle: Handle) -> Result<(), Error> {
    if *DISABLE_GENERATE_MISSING_THUMBNAILS {
        log::info!("generate_missing_thumbnails disabled");
        return Ok(());
    }
    tokio_handle.block_on(async {
        loop {
            let mut connection = acquire_db_connection().await?;

            let relevant_objects = run_serializable_transaction(&mut connection, |connection| async move {
                diesel::sql_query("
                WITH relevant_s3objects AS(
                    SELECT * FROM s3_object AS obj
                    WHERE NOT thumbnail_disabled
                    AND thumbnail_object_key IS NULL
                    AND (LOWER(mime_type) LIKE 'video/%' OR LOWER(mime_type) LIKE 'image/%' OR LOWER(mime_type) LIKE 'audio/%')
                    AND thumbnail_locked_at IS NULL
                    AND NOT (object_key LIKE 'thumb_%')
                    AND EXISTS(SELECT * FROM post WHERE s3_object = obj.object_key)
                    AND NOT EXISTS(SELECT * FROM s3_object WHERE thumbnail_object_key = obj.object_key)
                    AND NOT EXISTS(SELECT * FROM hls_stream WHERE stream_playlist = obj.object_key OR stream_file = obj.object_key OR master_playlist = obj.object_key)
                    AND (thumbnail_fail_count IS NULL OR thumbnail_fail_count < 3)
                    ORDER BY thumbnail_fail_count ASC NULLS FIRST, creation_timestamp ASC
                    LIMIT 100
                )
                UPDATE s3_object SET thumbnail_locked_at = NOW() WHERE thumbnail_locked_at IS NULL AND object_key IN(SELECT object_key FROM relevant_s3objects) RETURNING *;
            ")
            .load::<S3Object>(connection)
            .await
            .map_err(retry_on_serialization_failure)
            }.scope_boxed()).await?;
            drop(connection);

            if relevant_objects.is_empty() {
                break;
            }

            let _sentinel = LockedObjectsTaskSentinel::new(
                "thumbnail_locked_at",
                relevant_objects
                    .iter()
                    .map(|o| o.object_key.clone())
                    .collect::<Vec<String>>(),
                &tokio_handle,
            ).await;

            log::info!(
                "Found {} objects with missing thumbnails",
                relevant_objects.len()
            );
            for object in relevant_objects {
                if AtomicBool::load(&IS_SHUTDOWN, Ordering::Relaxed) {
                    log::warn!("Stopping task generate_missing_thumbnails because the task pool is shutting down");
                    return Ok(());
                }
                let mut connection = acquire_db_connection().await?;

                let (bucket, broker, user) =
                    match load_object_relations(object.fk_broker, object.fk_uploader, &mut connection).await {
                        Ok(res) => res,
                        Err(e) => {
                            log::error!("Failed to load data for {}: {}", &object.object_key, e);
                            continue;
                        }
                    };
                drop(connection);

                let file_id = match Path::new(&object.object_key)
                    .file_stem()
                    .map(|o| o.to_string_lossy().to_string())
                {
                    Some(file_id) => match Uuid::parse_str(&file_id) {
                        Ok(uuid) => uuid,
                        Err(e) => {
                            log::error!(
                                "Failed to get file stem for object key '{}': {}",
                                &object.object_key,
                                e
                            );
                            continue;
                        }
                    },
                    None => {
                        log::error!(
                            "Failed to get file stem for object key '{}'",
                            &object.object_key
                        );
                        continue;
                    }
                };

                if let Err(e) = encode::generate_thumbnail(
                    bucket,
                    object.object_key.clone(),
                    file_id,
                    object.mime_type,
                    broker,
                    user,
                    true,
                ).await {
                    log::error!(
                        "Failed generating thumbnail for object {}: {}",
                        &object.object_key,
                        e
                    );
                    if AtomicBool::load(&IS_SHUTDOWN, Ordering::Relaxed) {
                        log::warn!("Stopping task generate_missing_thumbnails because the task pool is shutting down");
                        return Ok(());
                    }
                    if let Ok(mut connection) = acquire_db_connection().await
                        && let Err(e) = diesel::sql_query("UPDATE s3_object SET thumbnail_fail_count = coalesce(thumbnail_fail_count, 0) + 1 WHERE object_key = $1")
                            .bind::<VarChar, _>(&object.object_key)
                            .execute(&mut connection).await {
                                log::error!("Failed to increment thumbnail_fail_count: {e}");
                    }
                } else {
                    log::info!(
                        "Generated missing thumbnail for object {}",
                        &object.object_key
                    );
                }
            }
        }

        Ok(())
    })
}

pub fn load_missing_object_metadata(tokio_handle: Handle) -> Result<(), Error> {
    if *DISABLE_LOAD_MISSING_METADATA {
        log::info!("load_missing_object_metadata disabled");
        return Ok(());
    }

    tokio_handle.block_on(async {
        loop {
            let mut connection = acquire_db_connection().await?;
            let relevant_objects = run_serializable_transaction(&mut connection, |connection| async {
                diesel::sql_query("
                WITH relevant_s3objects AS(
                    SELECT * FROM s3_object AS obj
                    WHERE NOT EXISTS(SELECT * FROM s3_object_metadata WHERE object_key = obj.object_key AND loaded)
                    AND metadata_locked_at IS NULL
                    AND NOT (object_key LIKE 'thumb_%')
                    AND EXISTS(SELECT * FROM post WHERE s3_object = obj.object_key)
                    AND NOT EXISTS(SELECT * FROM s3_object WHERE thumbnail_object_key = obj.object_key)
                    AND NOT EXISTS(SELECT * FROM hls_stream WHERE stream_playlist = obj.object_key OR stream_file = obj.object_key OR master_playlist = obj.object_key)
                    AND (metadata_fail_count IS NULL OR metadata_fail_count < 3)
                    ORDER BY metadata_fail_count ASC NULLS FIRST, creation_timestamp ASC
                    LIMIT 100
                )
                UPDATE s3_object SET metadata_locked_at = NOW() WHERE metadata_locked_at IS NULL AND object_key IN(SELECT object_key FROM relevant_s3objects) RETURNING *;
            ")
            .load::<S3Object>(connection)
            .await
            .map_err(retry_on_serialization_failure)
            }.scope_boxed()).await?;
            drop(connection);

            if relevant_objects.is_empty() {
                break;
            }

            let _sentinel = LockedObjectsTaskSentinel::new(
                "metadata_locked_at",
                relevant_objects
                    .iter()
                    .map(|o| o.object_key.clone())
                    .collect::<Vec<String>>(),
                &tokio_handle,
            ).await;

            log::info!("Found {} objects with missing metadata", relevant_objects.len());
            for object in relevant_objects {
                if AtomicBool::load(&IS_SHUTDOWN, Ordering::Relaxed) {
                    log::warn!("Stopping task load_missing_object_metadata because the task pool is shutting down");
                    return Ok(());
                }

                if let Err(e) = encode::load_object_metadata(object.object_key.clone(), true).await {
                    log::error!("Failed to load metadata for object {}: {}", &object.object_key, e);
                    if AtomicBool::load(&IS_SHUTDOWN, Ordering::Relaxed) {
                        log::warn!("Stopping task load_missing_object_metadata because the task pool is shutting down");
                        return Ok(());
                    }
                    if let Ok(mut connection) = acquire_db_connection().await
                        && let Err(e) = diesel::sql_query("UPDATE s3_object SET metadata_fail_count = coalesce(metadata_fail_count, 0) + 1 WHERE object_key = $1")
                            .bind::<VarChar, _>(&object.object_key)
                            .execute(&mut connection).await {
                                log::error!("Failed to increment metadata_fail_count: {e}");
                    }
                } else {
                    log::info!("Loaded missing metadata for object {}", &object.object_key);
                }
            }
        }

        Ok(())
    })
}

async fn load_object_relations(
    broker_pk: i64,
    user_pk: i64,
    connection: &mut AsyncPgConnection,
) -> Result<(Bucket, Broker, User), Error> {
    let broker = broker::table
        .filter(broker::pk.eq(broker_pk))
        .get_result::<Broker>(connection)
        .await?;

    let bucket = create_bucket(
        &broker.bucket,
        &broker.endpoint,
        &broker.access_key,
        &broker.secret_key,
        broker.is_aws_region,
    )?;

    let user = registered_user::table
        .filter(registered_user::pk.eq(user_pk))
        .get_result::<User>(connection)
        .await?;

    Ok((bucket, broker, user))
}

pub fn clear_old_object_locks(tokio_handle: Handle) -> Result<(), Error> {
    tokio_handle.block_on(async {
        let mut connection = acquire_db_connection().await?;

        run_serializable_transaction(&mut connection, |connection| async {
            // clear locks older than 1 hour in case a task failed to release them due to unexpected termination
            // locks are refreshed every 15 minutes, so locks older than 1 hour should be considered stale
            diesel::sql_query("UPDATE s3_object SET hls_locked_at = NULL WHERE hls_locked_at < NOW() - interval '1 hour'")
                .execute(connection)
                .await
                .map_err(retry_on_serialization_failure)?;

            diesel::sql_query("UPDATE s3_object SET thumbnail_locked_at = NULL WHERE thumbnail_locked_at < NOW() - interval '1 hour'")
                .execute(connection)
                .await
                .map_err(retry_on_serialization_failure)?;

            diesel::sql_query("UPDATE s3_object SET metadata_locked_at = NULL WHERE metadata_locked_at < NOW() - interval '1 hour'")
                .execute(connection)
                .await
                .map_err(retry_on_serialization_failure)?;

            diesel::sql_query("UPDATE deferred_s3_object_deletion SET locked_at = NULL WHERE locked_at < NOW() - interval '1 day'")
                .execute(connection)
                .await
                .map_err(retry_on_serialization_failure)?;

            diesel::sql_query("UPDATE apply_auto_tags_task SET locked_at = NULL WHERE locked_at < NOW() - interval '1 day'")
                .execute(connection)
                .await
                .map_err(retry_on_serialization_failure)?;

            diesel::sql_query("UPDATE reconcile_broker_quota_usage_task SET locked_at = NULL WHERE locked_at < NOW() - interval '1 day' AND NOT(fail_count < 3)")
                .execute(connection)
                .await
                .map_err(retry_on_serialization_failure)?;
            // avoid unlocking reconcile_broker_quota_usage_task violating unique index: "reconcile_broker_quota_usage_task_unique_idx" UNIQUE, btree (fk_user, fk_broker) WHERE locked_at IS NULL AND fail_count < 3
            // delete stale tasks where an unlocked task for the same user and broker exists, and deduplicate locked tasks
            diesel::sql_query(r#"
                WITH ranked AS (
                    SELECT
                        pk,
                        fk_user,
                        fk_broker,
                        ROW_NUMBER() OVER (
                            PARTITION BY fk_user, fk_broker
                            ORDER BY fail_count, pk
                        ) AS rn
                    FROM reconcile_broker_quota_usage_task
                    WHERE locked_at < NOW() - interval '1 day'
                      AND fail_count < 3
                ),
                deleted AS (
                    DELETE FROM reconcile_broker_quota_usage_task t
                    USING ranked r
                    WHERE t.pk = r.pk
                      AND (
                          r.rn > 1
                          OR EXISTS (
                              SELECT *
                              FROM reconcile_broker_quota_usage_task other
                              WHERE other.fk_user = r.fk_user
                                AND other.fk_broker = r.fk_broker
                                AND other.locked_at IS NULL
                                AND other.fail_count < 3
                          )
                      )
                    RETURNING t.pk
                )
                UPDATE reconcile_broker_quota_usage_task t
                SET locked_at = NULL
                FROM ranked r
                WHERE t.pk = r.pk
                  AND r.rn = 1
                  AND NOT EXISTS (
                      SELECT *
                      FROM deleted d
                      WHERE d.pk = t.pk
                  );
                "#)
                .execute(connection)
                .await?;

            diesel::sql_query("UPDATE broker SET quota_audit_locked_at = NULL WHERE quota_audit_locked_at < NOW() - interval '1 day'")
                .execute(connection)
                .await
                .map_err(retry_on_serialization_failure)?;

            Ok(())
        }.scope_boxed()).await
    })
}

pub fn clear_old_tokens(tokio_handle: Handle) -> Result<(), Error> {
    tokio_handle.block_on(async {
        let current_utc = Utc::now();
        let mut connection = acquire_db_connection().await?;

        diesel::delete(refresh_token::table)
            .filter(
                refresh_token::expiry
                    .lt(&current_utc)
                    .or(refresh_token::invalidated),
            )
            .execute(&mut connection)
            .await?;

        diesel::delete(email_confirmation_token::table)
            .filter(
                email_confirmation_token::expiry
                    .lt(&current_utc)
                    .or(email_confirmation_token::invalidated),
            )
            .execute(&mut connection)
            .await?;

        diesel::delete(one_time_password::table)
            .filter(
                one_time_password::expiry
                    .lt(&current_utc)
                    .or(one_time_password::invalidated),
            )
            .execute(&mut connection)
            .await?;

        Ok(())
    })
}

pub fn execute_deferred_s3_object_deletions(tokio_handle: Handle) -> Result<(), Error> {
    tokio_handle.block_on(async {
        loop {
            let mut connection = acquire_db_connection().await?;
            let deferred_deletions = run_serializable_transaction(&mut connection, |connection| async move {
                diesel::sql_query("
                WITH relevant_s3_object_deletions AS(
                    SELECT * FROM deferred_s3_object_deletion
                    WHERE locked_at IS NULL
                    AND (fail_count IS NULL OR fail_count < 3)
                    LIMIT 50
                )
                UPDATE deferred_s3_object_deletion SET locked_at = NOW() WHERE locked_at IS NULL AND object_key IN(SELECT object_key FROM relevant_s3_object_deletions) RETURNING *;
            ")
            .load::<DeferredS3ObjectDeletion>(connection)
            .await
            .map_err(retry_on_serialization_failure)
            }.scope_boxed()).await?;
            drop(connection);

            if deferred_deletions.is_empty() {
                break;
            }

            log::info!("Found {} s3 objects to delete", deferred_deletions.len());

            let _sentinel = LockedObjectsTaskSentinel::new_for_table(
                "locked_at",
                "deferred_s3_object_deletion",
                deferred_deletions
                    .iter()
                    .map(|o| o.object_key.clone())
                    .collect::<Vec<String>>(),
                &tokio_handle,
            ).await;

            for deferred_deletion in deferred_deletions {
                if AtomicBool::load(&IS_SHUTDOWN, Ordering::Relaxed) {
                    log::warn!("Stopping task execute_deferred_s3_object_deletions because the task pool is shutting down");
                    return Ok(());
                }
                let mut connection = acquire_db_connection().await?;
                let broker = broker::table
                    .filter(broker::pk.eq(deferred_deletion.fk_broker))
                    .get_result::<Broker>(&mut connection)
                    .await?;

                let bucket = create_bucket(
                    &broker.bucket,
                    &broker.endpoint,
                    &broker.access_key,
                    &broker.secret_key,
                    broker.is_aws_region,
                )?;

                match bucket.delete_object(&deferred_deletion.object_key).await {
                    Ok(response) if response.status_code() < 300 => {
                        log::info!("Deleted object {} from broker {} (s3 bucket {})", &deferred_deletion.object_key, broker.pk, &broker.bucket);
                        if let Err(e) = diesel::delete(deferred_s3_object_deletion::table)
                            .filter(deferred_s3_object_deletion::object_key.eq(&deferred_deletion.object_key))
                            .execute(&mut connection)
                            .await {
                                log::error!("Failed to delete deferred_s3_object_deletion for object {} after successful deletion with error: {e}", &deferred_deletion.object_key);
                            }
                    }
                    Ok(response) => {
                        if AtomicBool::load(&IS_SHUTDOWN, Ordering::Relaxed) {
                            log::warn!("Stopping task execute_deferred_s3_object_deletions because the task pool is shutting down");
                            return Ok(());
                        }
                        log::error!("Failed to delete object {} from s3 with status code {}", &deferred_deletion.object_key, response.status_code());
                        if let Err(e) = diesel::sql_query("UPDATE deferred_s3_object_deletion SET fail_count = coalesce(fail_count, 0) + 1 WHERE object_key = $1")
                        .bind::<VarChar, _>(&deferred_deletion.object_key)
                        .execute(&mut connection).await {
                            log::error!("Failed to increment fail_count: {e}");
                        }
                    }
                    Err(e) => {
                        if AtomicBool::load(&IS_SHUTDOWN, Ordering::Relaxed) {
                            log::warn!("Stopping task execute_deferred_s3_object_deletions because the task pool is shutting down");
                            return Ok(());
                        }
                        log::error!("Failed to delete object {} from s3 with error: {e}", &deferred_deletion.object_key);
                        if let Err(e) = diesel::sql_query("UPDATE deferred_s3_object_deletion SET fail_count = coalesce(fail_count, 0) + 1 WHERE object_key = $1")
                        .bind::<VarChar, _>(&deferred_deletion.object_key)
                        .execute(&mut connection).await {
                            log::error!("Failed to increment fail_count: {e}");
                        }
                    }
                }
            }
        }

        Ok(())
    })
}

pub fn run_apply_auto_tags_tasks(tokio_handle: Handle) -> Result<(), Error> {
    tokio_handle.block_on(async {
        loop {
            if AtomicBool::load(&IS_SHUTDOWN, Ordering::Relaxed) {
                log::warn!(
                    "Stopping task run_apply_auto_tags_tasks because the task pool is shutting down"
                );
            }

            let mut connection = acquire_db_connection().await?;
            let tasks = run_serializable_transaction(&mut connection, |connection| async move {
                diesel::sql_query("
                    WITH relevant_apply_auto_tags_tasks AS(
                        SELECT * FROM apply_auto_tags_task
                        WHERE locked_at IS NULL
                        AND fail_count < 3
                        ORDER BY fail_count ASC NULLS FIRST, creation_timestamp ASC
                        LIMIT 5
                    )
                    UPDATE apply_auto_tags_task SET locked_at = NOW() WHERE locked_at IS NULL AND pk IN(SELECT pk FROM relevant_apply_auto_tags_tasks) RETURNING *;
                ")
                .load::<ApplyAutoTagsTask>(connection)
                .await
                .map_err(retry_on_serialization_failure)
            }.scope_boxed()).await?;

            if tasks.is_empty() {
                break;
            }

            log::info!("Found {} apply_auto_tags_tasks to run", tasks.len());

            let _sentinel = LockedObjectsTaskSentinel::new_with_values(
                "locked_at",
                "apply_auto_tags_task",
                "pk",
                tasks.iter().map(|t| t.pk).collect::<Vec<i64>>(),
                &tokio_handle,
            ).await;

            for task in tasks {
                if AtomicBool::load(&IS_SHUTDOWN, Ordering::Relaxed) {
                    log::warn!("Stopping task run_apply_auto_tags_tasks because the task pool is shutting down");
                    return Ok(());
                }

                let task_to_run = task.clone();
                let res = run_serializable_transaction(&mut connection, |connection| async move{
                    run_apply_auto_tags_task(&task_to_run, connection).await
                }.scope_boxed()).await;

                if let Err(e) = res {
                    log::error!(
                        "Failed to run apply_auto_tags_task {task:?}: {e}");
                    let res = diesel::update(apply_auto_tags_task::table)
                        .filter(apply_auto_tags_task::pk.eq(task.pk))
                        .set(
                            apply_auto_tags_task::fail_count
                                .eq(apply_auto_tags_task::fail_count + 1),
                        )
                        .execute(&mut connection)
                        .await;
                    if let Err(e) = res {
                        log::error!(
                            "Failed to increment fail_count for apply_auto_tags_task {}: {e}",
                            task.pk
                        );
                    }
                } else {
                    let delete_task_res = diesel::delete(apply_auto_tags_task::table)
                        .filter(apply_auto_tags_task::pk.eq(task.pk))
                        .execute(&mut connection)
                        .await;
                    if let Err(e) = delete_task_res {
                        log::error!("Failed to delete apply_auto_tags_task {}: {e}", task.pk);
                    }
                }
            }
        }

        Ok(())
    })
}

pub fn run_reconcile_broker_quota_usage_tasks(tokio_handle: Handle) -> Result<(), Error> {
    tokio_handle.block_on(async {
        loop {
            if AtomicBool::load(&IS_SHUTDOWN, Ordering::Relaxed) {
                log::warn!(
                    "Stopping task run_reconcile_broker_quota_usage_tasks because the task pool is shutting down"
                );
            }

            let grace_period = RECONCILE_QUOTA_GRACE_PERIOD.to_postgres();

            let mut connection = acquire_db_connection().await?;
            let tasks = run_serializable_transaction(&mut connection, |connection| async {
                diesel::sql_query("
                    WITH relevant_reconcile_broker_quota_usage_tasks AS(
                        SELECT * FROM reconcile_broker_quota_usage_task
                        WHERE locked_at IS NULL
                        AND creation_timestamp < NOW() - $1::interval
                        AND fail_count < 3
                        ORDER BY fail_count ASC NULLS FIRST, creation_timestamp ASC
                        LIMIT 5
                    )
                    UPDATE reconcile_broker_quota_usage_task SET locked_at = NOW() WHERE locked_at IS NULL AND pk IN(SELECT pk FROM relevant_reconcile_broker_quota_usage_tasks) RETURNING *;
                ")
                .bind::<VarChar, _>(&grace_period)
                .load::<ReconcileBrokerQuotaUsageTask>(connection)
                .await
                .map_err(retry_on_serialization_failure)
            }.scope_boxed()).await?;

            if tasks.is_empty() {
                break;
            }

            log::info!("Found {} reconcile_broker_quota_usage_tasks to run (grace period: {})", tasks.len(), &grace_period);

            let task_keys = tasks.iter().map(|t| t.pk).collect::<Vec<i64>>();
            let sentinel = LockedObjectsTaskSentinel::new_with_values(
                "locked_at",
                "reconcile_broker_quota_usage_task",
                "pk",
                task_keys.clone(),
                &tokio_handle,
            ).await;

            for task in tasks {
                if AtomicBool::load(&IS_SHUTDOWN, Ordering::Relaxed) {
                    log::warn!("Stopping task run_reconcile_broker_quota_usage_tasks because the task pool is shutting down");
                    return Ok(());
                }

                let res = run_serializable_transaction(&mut connection, |connection| async move {
                    let user = registered_user::table.filter(registered_user::pk.eq(task.fk_user)).get_result::<User>(connection).await?;
                    let broker = broker::table.filter(broker::pk.eq(task.fk_broker)).get_result::<Broker>(connection).await?;
                    let system_user = get_system_user();

                    let quota = get_broker_access_quota(&broker, &user, connection).await?;
                    let mut used_quota = get_broker_quota_used_by_user(broker.pk, user.pk, connection).await?;

                    if let Some(quota) = quota && used_quota > quota as u128 {
                        log::info!("reconcile_broker_quota_usage_task: Quota exceeded for user '{}' ({}) on broker '{}' ({}) (quota: {}, used: {}), going to clean up posts", &user.user_name, user.pk, &broker.name, broker.pk, quota, used_quota);
                        let mut deleted_post_count = 0;

                        // First, delete all posts where the s3_object exceeds the quota
                        let (post_object, derived_object) = diesel::alias!(
                            s3_object as post_object,
                            s3_object as derived_object
                        );
                        let post_pks_to_delete = post::table
                            .inner_join(post_object)
                            .left_join(derived_object.on(
                                derived_object.field(s3_object::derived_from).eq(post_object.field(s3_object::object_key).nullable())
                                    .and(derived_object.field(s3_object::fk_broker).eq(broker.pk))
                            ))
                            .filter(
                                post::fk_create_user.eq(user.pk)
                                    .and(post_object.field(s3_object::fk_broker).eq(broker.pk))
                                    .and(post_object.field(s3_object::size_bytes).gt(quota).or(
                                        // also delete post if there is a derived object that exceeds quota
                                        derived_object.field(s3_object::size_bytes).gt(quota)
                                    ))
                            )
                            .select(post::pk)
                            .distinct()
                            .load::<i64>(connection)
                            .await?;

                        if !post_pks_to_delete.is_empty() {
                            log::debug!("reconcile_broker_quota_usage_task: Deleting following posts for user '{}' ({}) on broker '{}' ({}): {:?}", &user.user_name, user.pk, &broker.name, broker.pk, &post_pks_to_delete);
                            deleted_post_count += post_pks_to_delete.len();
                            for chunk in post_pks_to_delete.chunks(4096) {
                                execute_delete_posts(chunk, true, &system_user, connection).await?;
                            }
                            used_quota = get_broker_quota_used_by_user(broker.pk, user.pk, connection).await?;
                        }

                        while used_quota > quota as u128 {
                            let post_pk = post::table
                                .inner_join(s3_object::table)
                                .filter(
                                    post::fk_create_user.eq(user.pk)
                                        .and(s3_object::fk_broker.eq(broker.pk))
                                )
                                .select(post::pk)
                                .order_by(post::creation_timestamp.asc())
                                .limit(1)
                                .first::<i64>(connection)
                                .await
                                .optional()?;

                            if let Some(post_pk) = post_pk {
                                log::debug!("reconcile_broker_quota_usage_task: Deleting post with pk {} for user '{}' ({}) on broker '{}' ({})", post_pk, &user.user_name, user.pk, &broker.name, broker.pk);
                                deleted_post_count += 1;
                                execute_delete_posts(&[post_pk], true, &system_user, connection).await?;
                                used_quota = get_broker_quota_used_by_user(broker.pk, user.pk, connection).await?;
                            } else {
                                log::warn!("reconcile_broker_quota_usage_task: No posts left to delete for user '{}' ({}) on broker '{}' ({}) (quota: {:?}, used: {})", &user.user_name, user.pk, &broker.name, broker.pk, quota, used_quota);
                            }
                        }

                        log::info!("reconcile_broker_quota_usage_task: Deleted {} posts for user '{}' ({}) on broker '{}' ({}) (quota: {:?}, used: {})", deleted_post_count, &user.user_name, user.pk, &broker.name, broker.pk, quota, used_quota);
                    } else {
                        log::info!("reconcile_broker_quota_usage_task: Quota not exceeded for user '{}' ({}) on broker '{}' ({}) (quota: {:?}, used: {})", &user.user_name, user.pk, &broker.name, broker.pk, quota, used_quota);
                    }

                    Ok(())
                }.scope_boxed()).await;

                if let Err(e) = res {
                    log::error!(
                        "Failed to run reconcile_broker_quota_usage_task {task:?}: {e}");
                    let res = diesel::update(reconcile_broker_quota_usage_task::table)
                        .filter(reconcile_broker_quota_usage_task::pk.eq(task.pk))
                        .set(
                            reconcile_broker_quota_usage_task::fail_count
                                .eq(reconcile_broker_quota_usage_task::fail_count + 1),
                        )
                        .execute(&mut connection)
                        .await;
                    if let Err(e) = res {
                        log::error!(
                            "Failed to increment fail_count for reconcile_broker_quota_usage_task {}: {e}",
                            task.pk
                        );
                    }
                } else {
                    let delete_task_res = diesel::delete(reconcile_broker_quota_usage_task::table)
                        .filter(reconcile_broker_quota_usage_task::pk.eq(task.pk))
                        .execute(&mut connection)
                        .await;
                    if let Err(e) = delete_task_res {
                        log::error!("Failed to delete reconcile_broker_quota_usage_task {}: {e}", task.pk);
                    }
                }
            }

            sentinel.release_manually(async move {
                let res: Result<(), Error> = async {
                    let mut connection = acquire_db_connection().await?;
                    run_serializable_transaction(&mut connection, |connection| async {
                        // first delete tasks that have become obsolete because a new task for the same user/broker combination has been created while this task was locked
                        diesel::sql_query(r#"
                            DELETE FROM reconcile_broker_quota_usage_task task
                            WHERE pk = ANY($1)
                            AND EXISTS (
                                SELECT *
                                FROM reconcile_broker_quota_usage_task other
                                WHERE task.fk_user = other.fk_user
                                  AND task.fk_broker = other.fk_broker
                                  AND other.fail_count < 3
                                  AND other.pk != task.pk
                            )
                            "#)
                            .bind::<Array<BigInt>, _>(&task_keys)
                            .execute(connection)
                            .await?;

                        diesel::update(reconcile_broker_quota_usage_task::table)
                            .filter(reconcile_broker_quota_usage_task::pk.eq_any(&task_keys))
                            .set(reconcile_broker_quota_usage_task::locked_at.eq(None::<DateTime<Utc>>))
                            .execute(connection)
                            .await
                            .map_err(retry_on_constraint_violation)?;

                        Ok(())
                    }.scope_boxed()).await?;
                    Ok(())
                }.await;

                if let Err(e) = res {
                    log::error!("Failed to release reconcile_broker_quota_usage_task locks: {e}");
                } else {
                    log::debug!("Successfully released reconcile_broker_quota_usage_task locks for tasks {task_keys:?}");
                }
            });
        }

        Ok(())
    })
}

pub fn run_broker_quota_usage_audits(tokio_handle: Handle) -> Result<(), Error> {
    tokio_handle.block_on(async {
        loop {
            if AtomicBool::load(&IS_SHUTDOWN, Ordering::Relaxed) {
                log::warn!(
                    "Stopping task run_broker_quota_usage_audits because the task pool is shutting down"
                );
            }

            let mut connection = acquire_db_connection().await?;
            let broker_to_check = run_serializable_transaction(&mut connection, |connection| async {
                diesel::sql_query("
                    WITH broker_to_update AS(
                        SELECT * FROM broker
                        WHERE quota_audit_locked_at IS NULL
                        AND (last_quota_audit IS NULL OR last_quota_audit < NOW() - interval '1 day')
                        ORDER BY pk
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE broker SET quota_audit_locked_at = NOW() WHERE quota_audit_locked_at IS NULL AND pk IN(SELECT pk FROM broker_to_update) RETURNING *;
                ")
                .get_result::<Broker>(connection)
                .await
                .optional()
                .map_err(retry_on_serialization_failure)
            }.scope_boxed()).await?;

            let broker_to_check = match broker_to_check {
                Some(broker) => broker,
                None => break,
            };

            log::info!("quota audit: Auditing quota usage for broker '{}' ({})", broker_to_check.name, broker_to_check.pk);

            let _sentinel = LockedObjectsTaskSentinel::new_with_values(
                "quota_audit_locked_at",
                "broker",
                "pk",
                vec![broker_to_check.pk],
                &tokio_handle,
            ).await;

            let res = run_serializable_transaction(&mut connection, |connection| async {
                let broker_to_check = broker_to_check.clone();
                let mut violations_found = 0;

                #[derive(QueryableByName)]
                struct UserOverQuotaQueryResult {
                    #[diesel(sql_type = BigInt)]
                    user_pk: i64,
                }

                let user_over_quota_pks = diesel::sql_query("
                    WITH users AS (
                        SELECT DISTINCT fk_uploader AS user_pk
                        FROM s3_object
                        WHERE fk_broker = $1
                          AND fk_uploader <> $2
                    ),

                    quota_usage AS (
                        SELECT
                            fk_uploader AS user_pk,
                            SUM(size_bytes) FILTER (WHERE object_type = 'original') AS used_quota
                        FROM s3_object
                        WHERE fk_broker = $1
                          AND fk_uploader <> $2
                        GROUP BY fk_uploader
                    ),

                    user_applicable_access AS (
                        SELECT
                            u.user_pk,
                            ba.pk    AS broker_access_pk,
                            ba.quota AS quota,
                            ROW_NUMBER() OVER (
                                PARTITION BY u.user_pk
                                ORDER BY
                                    ba.fk_granted_user ASC NULLS LAST,
                                    ba.quota DESC NULLS FIRST
                            ) AS rn
                        FROM users u
                        JOIN broker_access ba
                          ON ba.fk_broker = $1

                        LEFT JOIN user_group_membership ugm
                          ON ugm.fk_group = ba.fk_granted_group
                         AND ugm.fk_user = u.user_pk
                         AND NOT ugm.revoked

                        WHERE (
                               ba.public
                            OR ba.fk_granted_user = u.user_pk
                            OR ugm.fk_user IS NOT NULL
                        )
                    ),

                    resolved_access AS (
                        SELECT
                            user_pk,
                            broker_access_pk,
                            quota
                        FROM user_applicable_access
                        WHERE rn = 1
                    )

                    SELECT
                        q.user_pk
                    FROM quota_usage q
                    LEFT JOIN resolved_access ra
                      ON ra.user_pk = q.user_pk
                    WHERE q.used_quota > COALESCE(ra.quota, 0);
                ")
                .bind::<BigInt, _>(broker_to_check.pk)
                .bind::<BigInt, _>(broker_to_check.fk_owner)
                .load::<UserOverQuotaQueryResult>(connection)
                .await?
                .into_iter()
                .map(|query_result| query_result.user_pk)
                .collect::<Vec<_>>();

                for user_pk in user_over_quota_pks {
                    let user = registered_user::table.filter(registered_user::pk.eq(user_pk)).get_result::<User>(connection).await?;

                    let quota = get_broker_access_quota(&broker_to_check, &user, connection).await?;
                    let used_quota = get_broker_quota_used_by_user(broker_to_check.pk, user.pk, connection).await?;

                    if let Some(quota) = quota && used_quota > quota as u128 {
                        let affected = diesel::insert_into(reconcile_broker_quota_usage_task::table)
                            .values(NewReconcileBrokerQuotaUsageTask {
                                fk_user: user_pk,
                                fk_broker: broker_to_check.pk,
                            })
                            .on_conflict_do_nothing()
                            .execute(connection)
                            .await?;

                        if affected > 0 {
                            violations_found += 1;
                            log::warn!("quota audit: Detected quota violation for user '{}' ({}) on broker '{}' ({}) (quota: {}, used: {}), creating reconcile_broker_quota_usage_task", &user.user_name, user.pk, &broker_to_check.name, broker_to_check.pk, quota, used_quota);
                        }
                    }
                }

                Ok(violations_found)
            }.scope_boxed()).await;

            match res {
                Ok(violations_found) => log::info!("quota audit: Quota audit completed for broker '{}' ({}), found {} violations", &broker_to_check.name, broker_to_check.pk, violations_found),
                Err(e) => log::error!("quota audit: Error occurred during quota audit for broker '{}' ({}): {}", &broker_to_check.name, broker_to_check.pk, e)
            }

            if let Err(e) = diesel::update(broker::table)
                .set(broker::last_quota_audit.eq(Utc::now()))
                .filter(broker::pk.eq(broker_to_check.pk))
                .execute(&mut connection)
                .await {
                log::error!("quota audit: Error updating last_quota_audit for broker '{}' ({}): {}", &broker_to_check.name, broker_to_check.pk, e);
            }
        }

        Ok(())
    })
}

pub trait RustTypeToSqlType:
    diesel::serialize::ToSql<Self::Sql, diesel::pg::Pg> + Clone + Sized + Send + Sync + 'static
where
    diesel::pg::Pg: diesel::sql_types::HasSqlType<Self::Sql>,
{
    type Sql: diesel::query_builder::QueryId + diesel::sql_types::SqlType + Send + 'static;
}

impl RustTypeToSqlType for String {
    type Sql = VarChar;
}

impl RustTypeToSqlType for i32 {
    type Sql = diesel::sql_types::Integer;
}

impl RustTypeToSqlType for i64 {
    type Sql = diesel::sql_types::BigInt;
}

pub struct LockedObjectsTaskSentinel<T: RustTypeToSqlType>
where
    diesel::pg::Pg: diesel::sql_types::HasSqlType<T::Sql>,
{
    pub lock_column: &'static str,
    pub table_name: &'static str,
    pub key_column: &'static str,
    pub object_keys: Vec<T>,
    pub refresh_task_join_handle: JoinHandle<()>,
    pub update_mutex: Arc<Mutex<()>>,
    manually_released: bool,
}

impl<T> LockedObjectsTaskSentinel<T>
where
    T: RustTypeToSqlType,
    diesel::pg::Pg: diesel::sql_types::HasSqlType<T::Sql>,
{
    pub async fn acquire(
        lock_column: &'static str,
        locked_column: &'static str,
        object_key: T,
    ) -> Result<Option<Self>, Error> {
        Self::acquire_with_condition(lock_column, &format!("{locked_column} IS NULL"), object_key)
            .await
    }

    pub async fn acquire_with_condition(
        lock_column: &'static str,
        condition: &str,
        object_key: T,
    ) -> Result<Option<Self>, Error> {
        Self::acquire_with_values(
            "s3_object",
            "object_key",
            lock_column,
            condition,
            object_key,
        )
        .await
    }

    pub async fn acquire_with_values(
        table_name: &'static str,
        key_column: &'static str,
        lock_column: &'static str,
        condition: &str,
        object_key: T,
    ) -> Result<Option<Self>, Error> {
        let mut connection = acquire_db_connection().await?;
        let update_result = diesel::sql_query(format!(
                "UPDATE {table_name} SET {lock_column} = NOW() WHERE {key_column} = $1 AND {lock_column} IS NULL AND {condition}",
            ))
            .bind::<<T as RustTypeToSqlType>::Sql, _>(&object_key)
            .execute(&mut connection)
            .await?;

        if update_result == 0 {
            return Ok(None);
        }

        Ok(Some(
            Self::new_with_values(
                lock_column,
                table_name,
                key_column,
                vec![object_key],
                &Handle::current(),
            )
            .await,
        ))
    }

    pub async fn new(
        lock_column: &'static str,
        object_keys: Vec<T>,
        tokio_handle: &Handle,
    ) -> Self {
        Self::new_for_table(lock_column, "s3_object", object_keys, tokio_handle).await
    }

    pub async fn new_for_table(
        lock_column: &'static str,
        table_name: &'static str,
        object_keys: Vec<T>,
        tokio_handle: &Handle,
    ) -> Self {
        Self::new_with_values(
            lock_column,
            table_name,
            "object_key",
            object_keys,
            tokio_handle,
        )
        .await
    }

    pub async fn new_with_values(
        lock_column: &'static str,
        table_name: &'static str,
        key_column: &'static str,
        object_keys: Vec<T>,
        tokio_handle: &Handle,
    ) -> Self {
        let keys_to_refresh = object_keys.clone();
        let update_mutex = Arc::new(Mutex::new(()));
        let background_mutex = update_mutex.clone();
        let refresh_task_join_handle = tokio_handle.spawn(async move {
            // refresh lock every minute
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                let _mutex_guard = background_mutex.lock().await;
                log::debug!("LockedObjectsTaskSentinel: Refreshing lock {lock_column} on {table_name} for key {key_column} in {:?}", &keys_to_refresh);
                match acquire_db_connection().await {
                    Ok(mut connection) => {
                        if let Err(e) = diesel::sql_query(format!(
                                "UPDATE {table_name} SET {lock_column} = NOW() WHERE {key_column} = ANY($1)"
                            ))
                            .bind::<Array<<T as RustTypeToSqlType>::Sql>, _>(&keys_to_refresh)
                            .execute(&mut connection)
                            .await
                        {
                            log::error!("Failed to refresh {lock_column} for objects: {e}");
                        }
                    }
                    Err(e) => log::error!("Failed to refresh {lock_column} for objects: {e}"),
                }
            }
        });

        Self {
            lock_column,
            table_name,
            object_keys,
            key_column,
            refresh_task_join_handle,
            update_mutex,
            manually_released: false,
        }
    }

    pub fn release_manually<F: Future + Send + 'static>(mut self, fut: F) {
        self.refresh_task_join_handle.abort();
        self.manually_released = true;
        tokio::spawn(async move {
            let _mutex = self.update_mutex.lock().await;
            fut.await;
        });
    }
}

impl<T> Drop for LockedObjectsTaskSentinel<T>
where
    T: RustTypeToSqlType,
    diesel::pg::Pg: diesel::sql_types::HasSqlType<T::Sql>,
{
    fn drop(&mut self) {
        if self.manually_released {
            return;
        }

        self.refresh_task_join_handle.abort();
        let update_mutex = self.update_mutex.clone();
        let object_keys = std::mem::take(&mut self.object_keys);
        let table_name = self.table_name;
        let key_column = self.key_column;
        let lock_column = self.lock_column;
        tokio::spawn(async move {
            let _update_mutex = update_mutex.lock().await;
            log::debug!(
                "LockedObjectsTaskSentinel: Releasing lock {lock_column} on {table_name} for key {key_column} in {:?}",
                &object_keys
            );
            let mut connection = match acquire_db_connection().await {
                Ok(connection) => connection,
                Err(e) => {
                    log::error!("Could not unlock objects: {e}");
                    return;
                }
            };

            let res = diesel::sql_query(format!(
                "UPDATE {table_name} SET {lock_column} = NULL WHERE {key_column} = ANY($1)",
            ))
            .bind::<Array<<T as RustTypeToSqlType>::Sql>, _>(&object_keys)
            .execute(&mut connection)
            .await;

            if let Err(e) = res {
                log::error!("Could not unlock objects: {e}");
            }
        });
    }
}
