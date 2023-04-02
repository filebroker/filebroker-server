use std::path::Path;

use chrono::Utc;
use diesel::{
    query_dsl::methods::FilterDsl,
    sql_types::{Array, VarChar},
    RunQueryDsl,
};
use rusty_pool::ThreadPool;
use s3::Bucket;
use tokio::{runtime::Handle, task::JoinHandle};

use lazy_static::lazy_static;
use uuid::Uuid;

use crate::{
    acquire_db_connection,
    data::{create_bucket, encode},
    diesel::ExpressionMethods,
    error::Error,
    model::{Broker, S3Object, User},
    schema::{broker, registered_user, s3_object},
    DbConnection,
};

lazy_static! {
    pub static ref TASK_POOL: ThreadPool = {
        let task_pool_worker_count = std::env::var("TASK_POOL_WORKER_COUNT")
            .map(|s| s.parse::<usize>().expect("TASK_POOL_WORKER_COUNT invalid"))
            .unwrap_or(4);
        rusty_pool::Builder::new()
            .core_size(task_pool_worker_count)
            .max_size(task_pool_worker_count)
            .name(String::from("task_pool"))
            .build()
    };
    pub static ref RUNNING_TASK_IDS: flurry::HashSet<&'static str> = flurry::HashSet::new();
    pub static ref DISABLE_GENERATE_MISSING_HLS_STREAMS: bool =
        std::env::var("DISABLE_GENERATE_MISSING_HLS_STREAMS")
            .map(|s| s
                .parse::<bool>()
                .expect("DISABLE_GENERATE_MISSING_HLS_STREAMS invalid"))
            .unwrap_or(false);
    pub static ref DISABLE_GENERATE_MISSING_THUMBNAILS: bool =
        std::env::var("DISABLE_GENERATE_MISSING_THUMBNAILS")
            .map(|s| s
                .parse::<bool>()
                .expect("DISABLE_GENERATE_MISSING_THUMBNAILS invalid"))
            .unwrap_or(false);
}

pub fn submit_task(
    task_id: &'static str,
    task: impl Fn(Handle) -> Result<(), Error> + Send + 'static,
) {
    let tokio_handle = Handle::current();
    TASK_POOL.execute(move || {
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
    });
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

pub fn generate_missing_hls_streams(tokio_handle: Handle) -> Result<(), Error> {
    if *DISABLE_GENERATE_MISSING_HLS_STREAMS {
        log::info!("generate_missing_hls_streams disabled");
        return Ok(());
    }
    let mut connection = acquire_db_connection()?;

    let relevant_objects = diesel::sql_query("
        WITH relevant_s3objects AS(
            SELECT * FROM s3_object AS obj
            WHERE NOT hls_disabled
            AND hls_master_playlist IS NULL
            AND LOWER(mime_type) LIKE 'video/%'
            AND hls_locked_at IS NULL
            AND EXISTS(SELECT * FROM broker WHERE pk = obj.fk_broker AND hls_enabled)
            AND NOT EXISTS(SELECT * FROM s3_object WHERE thumbnail_object_key = obj.object_key)
            AND NOT EXISTS(SELECT * FROM hls_stream WHERE stream_playlist = obj.object_key OR stream_file = obj.object_key OR master_playlist = obj.object_key)
            AND (hls_fail_count IS NULL OR hls_fail_count < 3)
            ORDER BY creation_timestamp
            LIMIT 25
        )
        UPDATE s3_object SET hls_locked_at = NOW() WHERE hls_locked_at IS NULL AND object_key IN(SELECT object_key FROM relevant_s3objects) RETURNING *;
    ")
    .load::<S3Object>(&mut connection)?;
    drop(connection);

    let _sentinel = LockedObjectTaskSentinel {
        lock_column: "hls_locked_at",
        object_keys: relevant_objects
            .iter()
            .map(|o| o.object_key.clone())
            .collect::<Vec<_>>(),
    };

    for object in relevant_objects {
        let mut connection = acquire_db_connection()?;

        let (bucket, broker, user) =
            match load_object_relations(object.fk_broker, object.fk_uploader, &mut connection) {
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

        let refresh_object_key = object.object_key.clone();
        let refresh_lock_handle = tokio_handle.spawn(async move {
            // refresh lock every 15 minutes
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60 * 15)).await;
                match acquire_db_connection() {
                    Ok(mut connection) => {
                        if let Err(e) = diesel::update(s3_object::table)
                            .set(s3_object::hls_locked_at.eq(Utc::now()))
                            .filter(s3_object::object_key.eq(&refresh_object_key))
                            .execute(&mut connection)
                        {
                            log::error!(
                                "Failed to refresh hls_locked_at for object {}: {e}",
                                &refresh_object_key
                            );
                        }
                    }
                    Err(e) => log::error!(
                        "Failed to refresh hls_locked_at for object {}: {e}",
                        &refresh_object_key
                    ),
                }
            }
        });
        let _refresh_lock_handle = ShutdownTaskOnDrop(refresh_lock_handle);

        if let Err(e) = tokio_handle.block_on(encode::generate_hls_playlist(
            bucket,
            object.object_key.clone(),
            file_id,
            broker,
            user,
        )) {
            log::error!(
                "Failed HLS transcoding of object {}: {}",
                &object.object_key,
                e
            );
            if let Ok(mut connection) = acquire_db_connection() {
                if let Err(e) = diesel::sql_query("UPDATE s3_object SET hls_fail_count = coalesce(hls_fail_count, 0) + 1 WHERE object_key = $1")
                    .bind::<VarChar, _>(&object.object_key)
                    .execute(&mut connection) {
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

    Ok(())
}

pub fn generate_missing_thumbnails(tokio_handle: Handle) -> Result<(), Error> {
    if *DISABLE_GENERATE_MISSING_THUMBNAILS {
        log::info!("generate_missing_thumbnails disabled");
        return Ok(());
    }
    let mut connection = acquire_db_connection()?;

    let relevant_objects = diesel::sql_query("
        WITH relevant_s3objects AS(
            SELECT * FROM s3_object AS obj
            WHERE thumbnail_object_key IS NULL
            AND (LOWER(mime_type) LIKE 'video/%' OR LOWER(mime_type) LIKE 'image/%')
            AND thumbnail_locked_at IS NULL
            AND NOT EXISTS(SELECT * FROM s3_object WHERE thumbnail_object_key = obj.object_key)
            AND NOT EXISTS(SELECT * FROM hls_stream WHERE stream_playlist = obj.object_key OR stream_file = obj.object_key OR master_playlist = obj.object_key)
            AND (thumbnail_fail_count IS NULL OR thumbnail_fail_count < 3)
            ORDER BY creation_timestamp
            LIMIT 100
        )
        UPDATE s3_object SET thumbnail_locked_at = NOW() WHERE thumbnail_locked_at IS NULL AND object_key IN(SELECT object_key FROM relevant_s3objects) RETURNING *;
    ")
    .load::<S3Object>(&mut connection)?;
    drop(connection);

    let _sentinel = LockedObjectTaskSentinel {
        lock_column: "thumbnail_locked_at",
        object_keys: relevant_objects
            .iter()
            .map(|o| o.object_key.clone())
            .collect::<Vec<_>>(),
    };

    for object in relevant_objects {
        let mut connection = acquire_db_connection()?;

        let (bucket, broker, user) =
            match load_object_relations(object.fk_broker, object.fk_uploader, &mut connection) {
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

        if let Err(e) = tokio_handle.block_on(encode::generate_thumbnail(
            bucket,
            object.object_key.clone(),
            file_id,
            object.mime_type,
            broker,
            user,
        )) {
            log::error!(
                "Failed generating thumbnail for object {}: {}",
                &object.object_key,
                e
            );
            if let Ok(mut connection) = acquire_db_connection() {
                if let Err(e) = diesel::sql_query("UPDATE s3_object SET thumbnail_fail_count = coalesce(thumbnail_fail_count, 0) + 1 WHERE object_key = $1")
                    .bind::<VarChar, _>(&object.object_key)
                    .execute(&mut connection) {
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

    Ok(())
}

fn load_object_relations(
    broker_pk: i32,
    user_pk: i32,
    connection: &mut DbConnection,
) -> Result<(Bucket, Broker, User), Error> {
    let broker = broker::table
        .filter(broker::pk.eq(broker_pk))
        .get_result::<Broker>(connection)?;

    let bucket = create_bucket(
        &broker.bucket,
        &broker.endpoint,
        &broker.access_key,
        &broker.secret_key,
        broker.is_aws_region,
    )?;

    let user = registered_user::table
        .filter(registered_user::pk.eq(user_pk))
        .get_result::<User>(connection)?;

    Ok((bucket, broker, user))
}

pub fn clear_old_object_locks(_tokio_handle: Handle) -> Result<(), Error> {
    let mut connection = acquire_db_connection()?;

    diesel::sql_query(
        "UPDATE s3_object SET hls_locked_at = NULL WHERE hls_locked_at < NOW() - interval '1 day'",
    )
    .execute(&mut connection)?;

    diesel::sql_query("UPDATE s3_object SET thumbnail_locked_at = NULL WHERE thumbnail_locked_at < NOW() - interval '1 day'").execute(&mut connection)?;

    Ok(())
}

struct LockedObjectTaskSentinel {
    lock_column: &'static str,
    object_keys: Vec<String>,
}

impl Drop for LockedObjectTaskSentinel {
    fn drop(&mut self) {
        let mut connection = match acquire_db_connection() {
            Ok(connection) => connection,
            Err(e) => {
                log::error!("Could not unlock objects: {}", e);
                return;
            }
        };

        let res = diesel::sql_query(format!(
            "UPDATE s3_object SET {} = NULL WHERE object_key = ANY($1)",
            self.lock_column
        ))
        .bind::<Array<VarChar>, _>(&self.object_keys)
        .execute(&mut connection);

        if let Err(e) = res {
            log::error!("Could not unlock objects: {}", e);
        }
    }
}

struct ShutdownTaskOnDrop<T>(JoinHandle<T>);

impl<T> Drop for ShutdownTaskOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
