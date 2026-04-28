pub mod broker_tasks;
pub mod object_tasks;
pub mod tag_tasks;

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use chrono::Utc;
use diesel::sql_types::{Array, VarChar};
use diesel_async::{RunQueryDsl, scoped_futures::ScopedFutureExt};
use rusty_pool::ThreadPool;
use tokio::{runtime::Handle, sync::Mutex, task::JoinHandle};

use lazy_static::lazy_static;

use crate::{
    acquire_db_connection,
    diesel::{BoolExpressionMethods, ExpressionMethods},
    error::Error,
    retry_on_serialization_failure, run_serializable_transaction,
    schema::{email_confirmation_token, one_time_password, refresh_token},
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
