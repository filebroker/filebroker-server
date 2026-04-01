use crate::error::{Error, TransactionRuntimeError};
use crate::model::{
    NewUserGroupAuditLog, NewUserGroupEditHistory, Tag, User, UserGroup, UserGroupAuditAction,
    UserGroupAuditLog, UserGroupEditHistory, UserGroupEditHistoryTag, UserGroupTag, UserPublic,
};
use crate::post::history::HistoryPaginationQueryParams;
use crate::schema::{
    registered_user, tag, user_group, user_group_audit_log, user_group_edit_history,
    user_group_edit_history_tag, user_group_tag,
};
use crate::tag::TagUsage;
use crate::user_group::update::UserGroupUpdateOptional;
use crate::user_group::{get_user_group_tags, load_user_group_detailed};
use crate::util::vec_eq_sorted;
use crate::{
    acquire_db_connection, perms, run_repeatable_read_transaction, run_serializable_transaction,
};
use chrono::{DateTime, Utc};
use diesel::{
    BelongingToDsl, ExpressionMethods, JoinOnDsl, NullableExpressionMethods, OptionalExtension,
    QueryDsl, Table,
};
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde::{Deserialize, Serialize};
use validator::Validate;
use warp::{Rejection, Reply};

#[derive(Serialize)]
pub struct UserGroupEditHistorySnapshot {
    pub pk: i64,
    pub fk_user_group: i64,
    pub edit_user: UserPublic,
    pub edit_timestamp: DateTime<Utc>,
    pub name: String,
    pub name_changed: bool,
    #[serde(rename = "is_public")]
    pub public: bool,
    pub public_changed: bool,
    pub description: Option<String>,
    pub description_changed: bool,
    pub allow_member_invite: bool,
    pub allow_member_invite_changed: bool,
    pub tags_changed: bool,
    pub tags: Vec<TagUsage>,
}

#[derive(Serialize)]
pub struct UserGroupEditHistoryResponse {
    pub edit_timestamp: DateTime<Utc>,
    pub edit_user: UserPublic,
    pub total_snapshot_count: i64,
    pub snapshots: Vec<UserGroupEditHistorySnapshot>,
}

pub async fn get_user_group_history_handler(
    pagination: HistoryPaginationQueryParams,
    user_group_pk: i64,
    user: User,
) -> Result<impl Reply, Rejection> {
    pagination.validate().map_err(|e| {
        Error::InvalidRequestInputError(format!(
            "Validation failed for HistoryPaginationQueryParams: {e}"
        ))
    })?;

    let mut connection = acquire_db_connection().await?;
    let response = run_repeatable_read_transaction(&mut connection, |connection| {
        async {
            let user_group =
                perms::load_user_group_secured(user_group_pk, Some(&user), connection).await?;
            if !perms::is_user_group_editable(user_group_pk, Some(&user), connection).await? {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InaccessibleObjectError(user_group_pk),
                ));
            }

            let total_snapshot_count = user_group_edit_history::table
                .filter(user_group_edit_history::fk_user_group.eq(user_group_pk))
                .count()
                .get_result::<i64>(connection)
                .await?;
            let limit = pagination.limit.unwrap_or(10);
            let offset = limit * pagination.page.unwrap_or(0);
            let user_group_edit_history_snapshots = user_group_edit_history::table
                .inner_join(registered_user::table)
                .filter(user_group_edit_history::fk_user_group.eq(user_group_pk))
                .order(user_group_edit_history::pk.desc())
                .limit(limit.into())
                .offset(offset.into())
                .load::<(UserGroupEditHistory, UserPublic)>(connection)
                .await?;

            let mut snapshots = Vec::new();
            for (user_group_edit_history_snapshot, edit_user) in user_group_edit_history_snapshots {
                let tags = get_user_group_snapshot_tags(
                    &user_group_edit_history_snapshot,
                    &user_group,
                    connection,
                )
                .await?;

                snapshots.push(UserGroupEditHistorySnapshot {
                    pk: user_group_edit_history_snapshot.pk,
                    fk_user_group: user_group_edit_history_snapshot.fk_user_group,
                    edit_user,
                    edit_timestamp: user_group_edit_history_snapshot.edit_timestamp,
                    name: user_group_edit_history_snapshot.name,
                    name_changed: user_group_edit_history_snapshot.name_changed,
                    public: user_group_edit_history_snapshot.public,
                    public_changed: user_group_edit_history_snapshot.public_changed,
                    description: user_group_edit_history_snapshot.description,
                    description_changed: user_group_edit_history_snapshot.description_changed,
                    allow_member_invite: user_group_edit_history_snapshot.allow_member_invite,
                    allow_member_invite_changed: user_group_edit_history_snapshot
                        .allow_member_invite_changed,
                    tags_changed: user_group_edit_history_snapshot.tags_changed,
                    tags,
                });
            }

            let edit_user = registered_user::table
                .filter(registered_user::pk.eq(user_group.fk_edit_user))
                .get_result::<UserPublic>(connection)
                .await
                .map_err(Error::from)?;

            Ok(UserGroupEditHistoryResponse {
                edit_timestamp: user_group.edit_timestamp,
                edit_user,
                total_snapshot_count,
                snapshots,
            })
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(&response))
}

async fn get_user_group_snapshot_tags(
    user_group_edit_history_snapshot: &UserGroupEditHistory,
    user_group: &UserGroup,
    connection: &mut AsyncPgConnection,
) -> Result<Vec<TagUsage>, Error> {
    // snapshots store the previous value and only store tags if they have changed,
    // thus we need to load the tags from the next snapshot where tags_changed is true,
    // or the group's current tags if no such snapshot exists
    let relevant_tag_snapshot = if user_group_edit_history_snapshot.tags_changed {
        Some(user_group_edit_history_snapshot.clone())
    } else {
        UserGroupEditHistory::belonging_to(user_group)
            .filter(user_group_edit_history::pk.gt(user_group_edit_history_snapshot.pk))
            .filter(user_group_edit_history::tags_changed)
            .order(user_group_edit_history::pk.asc())
            .first::<UserGroupEditHistory>(connection)
            .await
            .optional()
            .map_err(Error::from)?
    };
    let tags = if let Some(relevant_tag_snapshot) = relevant_tag_snapshot {
        UserGroupEditHistoryTag::belonging_to(&relevant_tag_snapshot)
            .inner_join(tag::table)
            .select((
                tag::table::all_columns(),
                user_group_edit_history_tag::auto_matched,
            ))
            .load::<(Tag, bool)>(connection)
            .await
            .map_err(Error::from)?
            .into_iter()
            .map(|(tag, auto_matched)| TagUsage { tag, auto_matched })
            .collect::<Vec<_>>()
    } else {
        get_user_group_tags(user_group.pk, connection).await?
    };

    Ok(tags)
}

pub async fn rewind_user_group_history_snapshot_handler(
    user_group_edit_history_pk: i64,
    user: User,
) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;
    let user_group = run_serializable_transaction(&mut connection, |connection| {
        async {
            let user_group_snapshot = user_group_edit_history::table
                .find(user_group_edit_history_pk)
                .get_result::<UserGroupEditHistory>(connection)
                .await
                .optional()
                .map_err(Error::from)?
                .ok_or(TransactionRuntimeError::Rollback(
                    Error::InaccessibleObjectError(user_group_edit_history_pk),
                ))?;

            let user_group = perms::load_user_group_secured(
                user_group_snapshot.fk_user_group,
                Some(&user),
                connection,
            )
            .await?;
            if !perms::is_user_group_editable(
                user_group_snapshot.fk_user_group,
                Some(&user),
                connection,
            )
            .await?
            {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InaccessibleObjectError(user_group_snapshot.fk_user_group),
                ));
            }

            let now = Utc::now();
            let update = UserGroupUpdateOptional {
                name: Some(user_group_snapshot.name.clone()),
                public: Some(user_group_snapshot.public),
                // `None` means "don't update this field", so if the field was empty in the snapshot, use an empty string to clear it
                description: Some(user_group_snapshot.description.clone().unwrap_or_default()),
                allow_member_invite: Some(user_group_snapshot.allow_member_invite),
                edit_timestamp: now,
                fk_edit_user: user.pk,
            };

            let mut curr_tags = get_user_group_tags(user_group.pk, connection).await?;
            let mut snapshot_tags =
                get_user_group_snapshot_tags(&user_group_snapshot, &user_group, connection).await?;
            let previous_tags = if !vec_eq_sorted(&mut curr_tags, &mut snapshot_tags) {
                diesel::delete(
                    user_group_tag::table.filter(user_group_tag::fk_user_group.eq(user_group.pk)),
                )
                .execute(connection)
                .await
                .map_err(Error::from)?;
                diesel::insert_into(user_group_tag::table)
                    .values(
                        snapshot_tags
                            .iter()
                            .map(|tag_usage| UserGroupTag {
                                fk_user_group: user_group.pk,
                                fk_tag: tag_usage.tag.pk,
                                auto_matched: tag_usage.auto_matched,
                            })
                            .collect::<Vec<_>>(),
                    )
                    .execute(connection)
                    .await
                    .map_err(Error::from)?;
                Some(curr_tags)
            } else {
                None
            };

            let update_field_changes = update.get_field_changes(&user_group);

            if update_field_changes.has_changes() || previous_tags.is_some() {
                let updated_user_group = diesel::update(user_group::table)
                    .filter(user_group::pk.eq(user_group.pk))
                    .set(&update)
                    .get_result::<UserGroup>(connection)
                    .await?;

                let user_group_edit_history = diesel::insert_into(user_group_edit_history::table)
                    .values(NewUserGroupEditHistory {
                        fk_user_group: user_group.pk,
                        fk_edit_user: user_group.fk_edit_user,
                        edit_timestamp: user_group.edit_timestamp,
                        name: user_group.name,
                        name_changed: update_field_changes.name_changed,
                        public: user_group.public,
                        public_changed: update_field_changes.public_changed,
                        description: user_group.description,
                        description_changed: update_field_changes.description_changed,
                        allow_member_invite: user_group.allow_member_invite,
                        allow_member_invite_changed: update_field_changes
                            .allow_member_invite_changed,
                        tags_changed: previous_tags.is_some(),
                    })
                    .get_result::<UserGroupEditHistory>(connection)
                    .await?;

                diesel::insert_into(user_group_audit_log::table)
                    .values(NewUserGroupAuditLog {
                        fk_user_group: user_group.pk,
                        fk_user: user.pk,
                        action: UserGroupAuditAction::Edit,
                        fk_target_user: None,
                        invite_code: None,
                        reason: None,
                        creation_timestamp: now,
                    })
                    .execute(connection)
                    .await?;

                if let Some(previous_tags) = previous_tags
                    && !previous_tags.is_empty()
                {
                    diesel::insert_into(user_group_edit_history_tag::table)
                        .values(
                            previous_tags
                                .iter()
                                .map(|tag_usage| UserGroupEditHistoryTag {
                                    fk_user_group_edit_history: user_group_edit_history.pk,
                                    fk_tag: tag_usage.tag.pk,
                                    auto_matched: tag_usage.auto_matched,
                                })
                                .collect::<Vec<_>>(),
                        )
                        .execute(connection)
                        .await?;
                }

                Ok(updated_user_group)
            } else {
                Ok(user_group.clone())
            }
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(
        &load_user_group_detailed(user_group, Some(&user), &mut connection).await?,
    ))
}

#[derive(Deserialize, Validate)]
pub struct AuditPaginationQueryParams {
    #[validate(range(min = 1, max = 100))]
    pub limit: Option<u32>,
    #[validate(range(min = 0, max = 1000))]
    pub page: Option<u32>,
}

#[derive(Serialize)]
pub struct UserGroupAuditLogInnerJoined {
    pub pk: i64,
    pub user: UserPublic,
    pub action: UserGroupAuditAction,
    pub target_user: Option<UserPublic>,
    pub invite_code: Option<String>,
    pub reason: Option<String>,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Serialize)]
pub struct GetUserGroupAuditLogsResponse {
    pub total_count: i64,
    pub audit_logs: Vec<UserGroupAuditLogInnerJoined>,
}

pub async fn get_user_group_audit_logs_handler(
    pagination: AuditPaginationQueryParams,
    user_group_pk: i64,
    user: User,
) -> Result<impl Reply, Rejection> {
    pagination.validate().map_err(|e| {
        Error::InvalidRequestInputError(format!(
            "Validation failed for AuditPaginationQueryParams: {e}"
        ))
    })?;

    let mut connection = acquire_db_connection().await?;
    let response = run_repeatable_read_transaction(&mut connection, |connection| {
        async {
            // only allow group admins / owner to view audit logs
            if !perms::is_user_group_editable(user_group_pk, Some(&user), connection).await? {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InaccessibleObjectError(user_group_pk),
                ));
            }

            let total_count = user_group_audit_log::table
                .filter(user_group_audit_log::fk_user_group.eq(user_group_pk))
                .count()
                .get_result::<i64>(connection)
                .await?;

            let limit = pagination.limit.unwrap_or(50);
            let offset = limit * pagination.page.unwrap_or(0);
            let (audit_log_user, audit_log_target_user) = diesel::alias!(
                registered_user as audit_log_user,
                registered_user as audit_log_target_user,
            );

            let logs = user_group_audit_log::table
                .inner_join(
                    audit_log_user.on(audit_log_user
                        .field(registered_user::pk)
                        .eq(user_group_audit_log::fk_user)),
                )
                .left_join(
                    audit_log_target_user.on(audit_log_target_user
                        .field(registered_user::pk)
                        .nullable()
                        .eq(user_group_audit_log::fk_target_user)),
                )
                .filter(user_group_audit_log::fk_user_group.eq(user_group_pk))
                .order(user_group_audit_log::pk.desc())
                .limit(limit.into())
                .offset(offset.into())
                .load::<(UserGroupAuditLog, UserPublic, Option<UserPublic>)>(connection)
                .await?;

            let audit_logs = logs
                .into_iter()
                .map(|(audit_log, audit_log_user, audit_log_target_user)| {
                    UserGroupAuditLogInnerJoined {
                        pk: audit_log.pk,
                        user: audit_log_user,
                        action: audit_log.action,
                        target_user: audit_log_target_user,
                        invite_code: audit_log.invite_code,
                        reason: audit_log.reason,
                        creation_timestamp: audit_log.creation_timestamp,
                    }
                })
                .collect::<Vec<_>>();

            Ok(GetUserGroupAuditLogsResponse {
                total_count,
                audit_logs,
            })
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(&response))
}
