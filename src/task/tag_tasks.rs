use crate::error::Error;
use crate::model::ApplyAutoTagsTask;
use crate::schema::apply_auto_tags_task;
use crate::tag::auto_matching::run_apply_auto_tags_task;
use crate::task::{IS_SHUTDOWN, LockedObjectsTaskSentinel};
use crate::{acquire_db_connection, retry_on_serialization_failure, run_serializable_transaction};
use diesel::ExpressionMethods;
use diesel_async::RunQueryDsl;
use diesel_async::scoped_futures::ScopedFutureExt;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::runtime::Handle;

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
                        "Failed to run apply_auto_tags_task {task:?}: {e}"
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
