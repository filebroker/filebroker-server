use crate::error::Error;
use crate::model::{ApplyAutoTagsTask, ReconcileTagMatchingCountsTask};
use crate::schema::{apply_auto_tags_task, reconcile_tag_matching_counts_task};
use crate::tag::auto_matching::run_apply_auto_tags_task;
use crate::tag::match_counting::{
    ReconcileTagMatchingCountsOutcome, run_reconcile_tag_matching_counts_task,
};
use crate::task::{
    IS_SHUTDOWN, LockedObjectsTaskSentinel, RECONCILE_TAG_MATCHING_COUNTS_GRACE_PERIOD,
};
use crate::{acquire_db_connection, retry_on_serialization_failure, run_serializable_transaction};
use diesel::ExpressionMethods;
use diesel::sql_types::{Array, BigInt, VarChar};
use diesel_async::RunQueryDsl;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::runtime::Handle;

pub fn run_apply_auto_tags_tasks(tokio_handle: Handle) -> Result<(), Error> {
    tokio_handle.block_on(async {
        loop {
            if AtomicBool::load(&IS_SHUTDOWN, Ordering::Relaxed) {
                log::warn!(
                    "Stopping task run_apply_auto_tags_tasks because the task pool is shutting down"
                );
                break;
            }

            let mut connection = acquire_db_connection().await?;
            let tasks = run_serializable_transaction(&mut connection, async |connection| {
                diesel::sql_query("
                    WITH relevant_apply_auto_tags_tasks AS(
                        SELECT * FROM apply_auto_tags_task
                        WHERE locked_at IS NULL
                        AND fail_count < 3
                        ORDER BY fail_count ASC, creation_timestamp ASC
                        LIMIT 5
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE apply_auto_tags_task SET locked_at = NOW() WHERE locked_at IS NULL AND pk IN(SELECT pk FROM relevant_apply_auto_tags_tasks) RETURNING *;
                ")
                .load::<ApplyAutoTagsTask>(connection)
                .await
                .map_err(retry_on_serialization_failure)
            }).await?;

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
                let res = run_apply_auto_tags_task(&task_to_run, &mut connection).await;

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

pub fn run_reconcile_tag_matching_counts_tasks(tokio_handle: Handle) -> Result<(), Error> {
    tokio_handle.block_on(async {
        let grace_period = RECONCILE_TAG_MATCHING_COUNTS_GRACE_PERIOD.to_postgres();
        let mut processed_task_pks = Vec::<i64>::new();

        loop {
            if AtomicBool::load(&IS_SHUTDOWN, Ordering::Relaxed) {
                log::warn!(
                    "Stopping task run_reconcile_tag_matching_counts_tasks because the task pool is shutting down"
                );
                break;
            }

            let mut connection = acquire_db_connection().await?;

            let tasks = run_serializable_transaction(&mut connection, async |connection| {
                diesel::sql_query("
                    WITH relevant_tasks AS (
                        SELECT pk
                        FROM reconcile_tag_matching_counts_task
                        WHERE locked_at IS NULL
                          AND fail_count < 3
                          AND creation_timestamp < NOW() - $1::INTERVAL
                          AND NOT (pk = ANY($2))
                        ORDER BY fail_count ASC, creation_timestamp ASC
                        LIMIT 5
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE reconcile_tag_matching_counts_task AS task
                    SET locked_at = NOW()
                    FROM relevant_tasks
                    WHERE task.pk = relevant_tasks.pk
                    RETURNING task.*;
                ")
                .bind::<VarChar, _>(&grace_period)
                .bind::<Array<BigInt>, _>(&processed_task_pks)
                .load::<ReconcileTagMatchingCountsTask>(connection)
                .await
                .map_err(retry_on_serialization_failure)
            })
            .await?;

            if tasks.is_empty() {
                break;
            }

            processed_task_pks.extend(tasks.iter().map(|task| task.pk));

            log::info!(
                "Found {} reconcile_tag_matching_counts_tasks to run (grace period: {})",
                tasks.len(),
                grace_period
            );

            // Refresh locks throughout the batch and release any remaining
            // locks on drop. Invalidated tasks must not be deleted.
            let _sentinel = LockedObjectsTaskSentinel::new_with_values(
                "locked_at",
                "reconcile_tag_matching_counts_task",
                "pk",
                tasks.iter().map(|task| task.pk).collect::<Vec<_>>(),
                &tokio_handle,
            ).await;

            for task in tasks {
                if AtomicBool::load(&IS_SHUTDOWN, Ordering::Relaxed) {
                    log::warn!(
                        "Stopping task run_reconcile_tag_matching_counts_tasks because the task pool is shutting down"
                    );
                    return Ok(());
                }

                match run_reconcile_tag_matching_counts_task(&task, &mut connection).await {
                    Ok(ReconcileTagMatchingCountsOutcome::Completed) => {
                        log::debug!(
                            "Completed reconcile_tag_matching_counts_task {} for tag {}",
                            task.pk,
                            task.fk_tag
                        );
                    }
                    Ok(ReconcileTagMatchingCountsOutcome::Requeued) => {
                        log::debug!(
                            "Published counts for reconcile_tag_matching_counts_task {} but retained the task due to subsequent usage changes",
                            task.pk
                        );
                    }
                    Ok(ReconcileTagMatchingCountsOutcome::Discarded) => {
                        log::debug!(
                            "Discarded stale reconcile_tag_matching_counts_task {} because its matching topology changed or it no longer exists",
                            task.pk
                        );
                    }
                    Err(e) => {
                        log::error!(
                            "Failed to run reconcile_tag_matching_counts_task {task:?}: {e}"
                        );

                        let result = diesel::update(reconcile_tag_matching_counts_task::table)
                            .filter(reconcile_tag_matching_counts_task::pk.eq(task.pk))
                            .filter(
                                reconcile_tag_matching_counts_task::topology_version
                                    .eq(task.topology_version),
                            )
                            .set(
                                reconcile_tag_matching_counts_task::fail_count
                                    .eq(reconcile_tag_matching_counts_task::fail_count + 1),
                            )
                            .execute(&mut connection)
                            .await;

                        if let Err(e) = result {
                            log::error!(
                                "Failed to increment fail_count for reconcile_tag_matching_counts_task {}: {e}",
                                task.pk
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    })
}
