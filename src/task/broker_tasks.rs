use crate::broker::{get_broker_access_quota, get_broker_quota_used_by_user};
use crate::error::Error;
use crate::model::{
    Broker, NewReconcileBrokerQuotaUsageTask, ReconcileBrokerQuotaUsageTask, User, get_system_user,
};
use crate::post::delete::execute_delete_posts;
use crate::schema::{broker, post, reconcile_broker_quota_usage_task, registered_user, s3_object};
use crate::task::{IS_SHUTDOWN, LockedObjectsTaskSentinel, RECONCILE_QUOTA_GRACE_PERIOD};
use crate::{
    acquire_db_connection, retry_on_constraint_violation, retry_on_serialization_failure,
    run_serializable_transaction,
};
use chrono::{DateTime, Utc};
use diesel::sql_types::{Array, BigInt, VarChar};
use diesel::{
    BoolExpressionMethods, ExpressionMethods, JoinOnDsl, NullableExpressionMethods,
    OptionalExtension, QueryDsl,
};
use diesel_async::{RunQueryDsl, scoped_futures::ScopedFutureExt};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::runtime::Handle;

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
