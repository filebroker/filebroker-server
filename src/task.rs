use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use chrono::Utc;
use diesel::{
    QueryDsl,
    sql_types::{Array, VarChar},
};
use diesel_async::{AsyncPgConnection, RunQueryDsl, scoped_futures::ScopedFutureExt};
use rusty_pool::ThreadPool;
use s3::Bucket;
use tokio::{runtime::Handle, sync::Mutex, task::JoinHandle};

use lazy_static::lazy_static;
use uuid::Uuid;

use crate::schema::apply_auto_tags_task;
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
    retry_on_serialization_failure, run_serializable_transaction,
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
                log::error!("Error executing task {task_id}: {}", e);
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
                    if let Ok(mut connection) = acquire_db_connection().await {
                        if let Err(e) = diesel::sql_query("UPDATE s3_object SET hls_fail_count = coalesce(hls_fail_count, 0) + 1 WHERE object_key = $1")
                            .bind::<VarChar, _>(&object.object_key)
                            .execute(&mut connection).await {
                                log::error!("Failed to increment hls_fail_count: {e}");
                            }
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
                    if let Ok(mut connection) = acquire_db_connection().await {
                        if let Err(e) = diesel::sql_query("UPDATE s3_object SET thumbnail_fail_count = coalesce(thumbnail_fail_count, 0) + 1 WHERE object_key = $1")
                            .bind::<VarChar, _>(&object.object_key)
                            .execute(&mut connection).await {
                                log::error!("Failed to increment thumbnail_fail_count: {e}");
                            }
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
                    if let Ok(mut connection) = acquire_db_connection().await {
                        if let Err(e) = diesel::sql_query("UPDATE s3_object SET metadata_fail_count = coalesce(metadata_fail_count, 0) + 1 WHERE object_key = $1")
                            .bind::<VarChar, _>(&object.object_key)
                            .execute(&mut connection).await {
                                log::error!("Failed to increment metadata_fail_count: {e}");
                            }
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
                        "Failed to run apply_auto_tags_task {:?}: {e}",
                        task
                    );
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
        }
    }
}

impl<T> Drop for LockedObjectsTaskSentinel<T>
where
    T: RustTypeToSqlType,
    diesel::pg::Pg: diesel::sql_types::HasSqlType<T::Sql>,
{
    fn drop(&mut self) {
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
                    log::error!("Could not unlock objects: {}", e);
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
                log::error!("Could not unlock objects: {}", e);
            }
        });
    }
}
