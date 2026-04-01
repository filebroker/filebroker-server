use crate::diesel::{BoolExpressionMethods, ExpressionMethods, QueryDsl, Table};
use crate::error::{Error, TransactionRuntimeError};
use crate::model::{
    NewUserGroupAuditLog, NewUserGroupEditHistory, Tag, User, UserGroup, UserGroupAuditAction,
    UserGroupEditHistory, UserGroupEditHistoryTag, UserGroupMembership, UserGroupTag,
};
use crate::post::update::handle_object_tag_update;
use crate::query::load_and_report_missing_pks;
use crate::schema::{
    tag, user_group, user_group_audit_log, user_group_edit_history, user_group_edit_history_tag,
    user_group_membership, user_group_tag,
};
use crate::tag::create::get_or_create_tags;
use crate::tag::{
    TagUsage, filter_redundant_tags, get_source_object_tag, sanitize_request_tags, validate_tags,
};
use crate::user_group::{
    UserGroupJoined, get_user_group_joined, is_user_group_detailed_editable,
    load_user_group_detailed,
};
use crate::util::{dedup_vec, dedup_vec_optional, dedup_vecs_optional, string_value_updated};
use crate::{acquire_db_connection, perms, run_serializable_transaction};
use chrono::{DateTime, Utc};
use diesel::OptionalExtension;
use diesel_async::RunQueryDsl;
use diesel_async::scoped_futures::ScopedFutureExt;
use serde::{Deserialize, Serialize};
use validator::Validate;
use warp::{Rejection, Reply};

#[derive(AsChangeset)]
#[diesel(table_name = user_group)]
pub struct UserGroupUpdateOptional {
    pub name: Option<String>,
    pub public: Option<bool>,
    pub description: Option<String>,
    pub allow_member_invite: Option<bool>,
    pub edit_timestamp: DateTime<Utc>,
    pub fk_edit_user: i64,
}

impl UserGroupUpdateOptional {
    pub fn get_field_changes(&self, curr_value: &UserGroup) -> UserGroupUpdateFieldChanges {
        UserGroupUpdateFieldChanges {
            name_changed: string_value_updated(Some(&curr_value.name), self.name.as_deref()),
            public_changed: self.public.map(|v| v != curr_value.public).unwrap_or(false),
            description_changed: string_value_updated(
                curr_value.description.as_deref(),
                self.description.as_deref(),
            ),
            allow_member_invite_changed: self
                .allow_member_invite
                .map(|v| v != curr_value.allow_member_invite)
                .unwrap_or(false),
        }
    }

    pub fn has_changes(&self, curr_value: &UserGroup) -> bool {
        self.get_field_changes(curr_value).has_changes()
    }
}

pub struct UserGroupUpdateFieldChanges {
    pub name_changed: bool,
    pub public_changed: bool,
    pub description_changed: bool,
    pub allow_member_invite_changed: bool,
}

impl UserGroupUpdateFieldChanges {
    pub fn has_changes(&self) -> bool {
        self.name_changed
            || self.public_changed
            || self.description_changed
            || self.allow_member_invite_changed
    }
}

#[derive(Clone, Deserialize, Validate)]
pub struct EditUserGroupRequest {
    #[validate(length(max = 255))]
    pub name: Option<String>,
    pub is_public: Option<bool>,
    #[validate(length(max = 30000))]
    pub description: Option<String>,
    pub allow_member_invite: Option<bool>,
    #[validate(length(max = 100), custom(function = "validate_tags"))]
    pub tags_overwrite: Option<Vec<String>>,
    #[validate(length(max = 100))]
    pub tag_pks_overwrite: Option<Vec<i64>>,
    pub removed_tag_pks: Option<Vec<i64>>,
    #[validate(length(max = 100))]
    pub added_tag_pks: Option<Vec<i64>>,
    #[validate(length(max = 100), custom(function = "validate_tags"))]
    pub added_tags: Option<Vec<String>>,
}

pub async fn edit_user_group_handler(
    mut request: EditUserGroupRequest,
    user_group_pk: i64,
    user: User,
) -> Result<impl Reply, Rejection> {
    request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for EditUserGroupRequest: {e}"
        )))
    })?;

    dedup_vec_optional(&mut request.tags_overwrite);
    dedup_vec_optional(&mut request.tag_pks_overwrite);
    dedup_vec_optional(&mut request.removed_tag_pks);
    dedup_vec_optional(&mut request.added_tag_pks);
    dedup_vec_optional(&mut request.added_tags);
    dedup_vecs_optional(&mut request.added_tag_pks, &request.tag_pks_overwrite);
    dedup_vecs_optional(&mut request.added_tags, &request.tags_overwrite);

    let mut connection = acquire_db_connection().await?;
    let user_group_detailed = run_serializable_transaction(&mut connection, |connection| {
        async move {
            let user_group =
                perms::load_user_group_secured(user_group_pk, Some(&user), connection).await?;
            if !perms::is_user_group_editable(user_group_pk, Some(&user), connection).await? {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InaccessibleObjectError(user_group_pk),
                ));
            }

            let previous_tags = handle_object_tag_update!(
                user_group_pk,
                UserGroupTag,
                user_group_tag,
                fk_user_group,
                request.tags_overwrite,
                request.tag_pks_overwrite,
                request.added_tags,
                request.added_tag_pks,
                request.removed_tag_pks,
                connection,
                &user
            );

            let now = Utc::now();
            let update = UserGroupUpdateOptional {
                name: request.name,
                public: request.is_public,
                description: request.description,
                allow_member_invite: request.allow_member_invite,
                edit_timestamp: now,
                fk_edit_user: user.pk,
            };

            let update_field_changes = update.get_field_changes(&user_group);

            if update_field_changes.has_changes() || previous_tags.is_some() {
                let updated_user_group = diesel::update(user_group::table)
                    .filter(user_group::pk.eq(user_group.pk))
                    .set(update)
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

                Ok(load_user_group_detailed(updated_user_group, Some(&user), connection).await?)
            } else {
                Ok(load_user_group_detailed(user_group, Some(&user), connection).await?)
            }
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(&user_group_detailed))
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum MembershipChangeAction {
    Promote,
    Demote,
    Kick { reason: Option<String> },
    Ban { reason: Option<String> },
    Unban,
}

#[derive(Serialize)]
pub struct ChangeUserGroupMembershipResponse {
    pub prev: UserGroupMembership,
    pub updated: Option<UserGroupMembership>,
    pub changed: bool,
}

pub async fn change_user_group_membership_handler(
    user_group_pk: i64,
    member_user_pk: i64,
    action: MembershipChangeAction,
    user: User,
) -> Result<impl Reply, Rejection> {
    match action {
        MembershipChangeAction::Kick {
            reason: Some(ref reason),
        }
        | MembershipChangeAction::Ban {
            reason: Some(ref reason),
        } if reason.len() > 30000 => {
            return Err(warp::reject::custom(Error::InvalidRequestInputError(
                format!(
                    "Reason with length {} exceeds maximum length of 30000",
                    reason.len()
                ),
            )));
        }
        _ => {}
    }

    let mut connection = acquire_db_connection().await?;
    let response = run_serializable_transaction(&mut connection, |connection| {
        async move {
            let UserGroupJoined { group, membership } =
                get_user_group_joined(user_group_pk, Some(&user), connection).await?;
            match action {
                MembershipChangeAction::Promote | MembershipChangeAction::Demote => {
                    // only owner can promote/demote admins
                    if group.owner.pk != user.pk {
                        return Err(TransactionRuntimeError::Rollback(
                            Error::InaccessibleObjectError(user_group_pk),
                        ));
                    }
                }
                MembershipChangeAction::Kick { .. }
                | MembershipChangeAction::Ban { .. }
                | MembershipChangeAction::Unban => {
                    // owner / admins can kick/ban
                    if !is_user_group_detailed_editable(&group, &user, membership.as_ref()) {
                        return Err(TransactionRuntimeError::Rollback(
                            Error::InaccessibleObjectError(user_group_pk),
                        ));
                    }
                }
            }

            let prev = user_group_membership::table
                .filter(
                    user_group_membership::fk_user
                        .eq(member_user_pk)
                        .and(user_group_membership::fk_group.eq(group.pk)),
                )
                .get_result::<UserGroupMembership>(connection)
                .await
                .optional()?
                .ok_or(Error::NotFoundError)?;

            let updated = match action {
                MembershipChangeAction::Promote => {
                    if prev.revoked {
                        return Err(TransactionRuntimeError::Rollback(Error::BadRequestError(
                            format!(
                                "Cannot promote banned user {} from group {}",
                                member_user_pk, user_group_pk
                            ),
                        )));
                    }
                    if prev.administrator {
                        return Ok(ChangeUserGroupMembershipResponse {
                            prev,
                            updated: None,
                            changed: false,
                        });
                    }

                    Some(
                        diesel::update(user_group_membership::table)
                            .filter(
                                user_group_membership::fk_user
                                    .eq(member_user_pk)
                                    .and(user_group_membership::fk_group.eq(group.pk)),
                            )
                            .set(user_group_membership::administrator.eq(true))
                            .get_result::<UserGroupMembership>(connection)
                            .await?,
                    )
                }
                MembershipChangeAction::Demote => {
                    if prev.revoked {
                        return Err(TransactionRuntimeError::Rollback(Error::BadRequestError(
                            format!(
                                "Cannot demote banned user {} from group {}",
                                member_user_pk, user_group_pk
                            ),
                        )));
                    }
                    if !prev.administrator {
                        return Ok(ChangeUserGroupMembershipResponse {
                            prev,
                            updated: None,
                            changed: false,
                        });
                    }

                    Some(
                        diesel::update(user_group_membership::table)
                            .filter(
                                user_group_membership::fk_user
                                    .eq(member_user_pk)
                                    .and(user_group_membership::fk_group.eq(group.pk)),
                            )
                            .set(user_group_membership::administrator.eq(false))
                            .get_result::<UserGroupMembership>(connection)
                            .await?,
                    )
                }
                MembershipChangeAction::Kick { .. } => {
                    if prev.revoked {
                        return Err(TransactionRuntimeError::Rollback(Error::BadRequestError(
                            format!(
                                "Cannot kick banned user {} from group {}",
                                member_user_pk, user_group_pk
                            ),
                        )));
                    }

                    diesel::delete(user_group_membership::table)
                        .filter(
                            user_group_membership::fk_user
                                .eq(member_user_pk)
                                .and(user_group_membership::fk_group.eq(group.pk)),
                        )
                        .execute(connection)
                        .await?;

                    None
                }
                MembershipChangeAction::Ban { .. } => {
                    if prev.revoked {
                        return Ok(ChangeUserGroupMembershipResponse {
                            prev,
                            updated: None,
                            changed: false,
                        });
                    }

                    Some(
                        diesel::update(user_group_membership::table)
                            .filter(
                                user_group_membership::fk_user
                                    .eq(member_user_pk)
                                    .and(user_group_membership::fk_group.eq(group.pk)),
                            )
                            .set(user_group_membership::revoked.eq(true))
                            .get_result::<UserGroupMembership>(connection)
                            .await?,
                    )
                }
                MembershipChangeAction::Unban => {
                    if !prev.revoked {
                        return Ok(ChangeUserGroupMembershipResponse {
                            prev,
                            updated: None,
                            changed: false,
                        });
                    }

                    diesel::delete(user_group_membership::table)
                        .filter(
                            user_group_membership::fk_user
                                .eq(member_user_pk)
                                .and(user_group_membership::fk_group.eq(group.pk)),
                        )
                        .execute(connection)
                        .await?;

                    None
                }
            };

            let audit_action = match action {
                MembershipChangeAction::Promote => UserGroupAuditAction::AdminPromote,
                MembershipChangeAction::Demote => UserGroupAuditAction::AdminDemote,
                MembershipChangeAction::Kick { .. } => UserGroupAuditAction::Kick,
                MembershipChangeAction::Ban { .. } => UserGroupAuditAction::Ban,
                MembershipChangeAction::Unban => UserGroupAuditAction::Unban,
            };

            let reason = match action {
                MembershipChangeAction::Promote
                | MembershipChangeAction::Demote
                | MembershipChangeAction::Unban => None,
                MembershipChangeAction::Kick { reason }
                | MembershipChangeAction::Ban { reason } => reason,
            };

            diesel::insert_into(user_group_audit_log::table)
                .values(NewUserGroupAuditLog {
                    fk_user_group: group.pk,
                    fk_user: user.pk,
                    action: audit_action,
                    fk_target_user: Some(member_user_pk),
                    invite_code: None,
                    reason,
                    creation_timestamp: Utc::now(),
                })
                .execute(connection)
                .await?;

            Ok(ChangeUserGroupMembershipResponse {
                prev,
                updated,
                changed: true,
            })
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(&response))
}
