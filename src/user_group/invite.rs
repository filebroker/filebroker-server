use crate::diesel::BoolExpressionMethods;
use crate::diesel::{ExpressionMethods, QueryDsl};
use crate::error::{Error, TransactionRuntimeError};
use crate::model::{
    NewUserGroupAuditLog, User, UserGroup, UserGroupAuditAction, UserGroupInvite,
    UserGroupMembership, UserPublic,
};
use crate::perms::get_group_membership_condition;
use crate::query::{ApplyOrderFn, apply_key_ordering, order_by_col_fn, order_by_col_with_tie_fn};
use crate::schema::{
    registered_user, user_group, user_group_audit_log, user_group_invite, user_group_membership,
};
use crate::user_group::{
    UserGroupDetailed, UserGroupJoined, get_user_group_joined, is_user_group_detailed_editable,
    is_user_group_editable,
};
use crate::{
    acquire_db_connection, perms, retry_on_constraint_violation, run_repeatable_read_transaction,
    run_serializable_transaction,
};
use chrono::{DateTime, Days, Utc};
use diesel::dsl::not;
use diesel::query_builder::BoxedSelectStatement;
use diesel::sql_types::Bool;
use diesel::{IntoSql, JoinOnDsl, NullableExpressionMethods, OptionalExtension, Table};
use diesel_async::RunQueryDsl;
use diesel_async::scoped_futures::ScopedFutureExt;
use rand::distr::{Alphanumeric, SampleString};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use validator::Validate;
use warp::{Rejection, Reply};

#[derive(Serialize)]
pub struct UserGroupInviteDetailed {
    pub code: String,
    pub user_group: UserGroupDetailed,
    pub create_user: UserPublic,
    pub invited_user: Option<UserPublic>,
    pub creation_timestamp: DateTime<Utc>,
    pub expiration_timestamp: Option<DateTime<Utc>>,
    pub last_used_timestamp: Option<DateTime<Utc>>,
    pub max_uses: Option<i32>,
    pub uses_count: i32,
    pub revoked: bool,
}

#[derive(Clone, Deserialize, Validate)]
pub struct CreateUserGroupInviteRequest {
    pub invited_user_pk: Option<i64>,
    #[validate(range(min = 1, max = 3600))]
    pub expiration_days: Option<u16>,
    #[validate(range(min = 1))]
    pub max_uses: Option<i32>,
}

pub async fn create_user_group_invite_handler(
    user_group_pk: i64,
    request: CreateUserGroupInviteRequest,
    user: User,
) -> Result<impl Reply, Rejection> {
    request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for CreateUserGroupInviteRequest: {e}"
        )))
    })?;

    let mut connection = acquire_db_connection().await?;
    let invited_user = if let Some(invited_user_pk) = request.invited_user_pk {
        Some(
            registered_user::table
                .filter(registered_user::pk.eq(invited_user_pk))
                .get_result::<User>(&mut connection)
                .await
                .optional()
                .map_err(Error::from)?
                .ok_or(Error::InaccessibleObjectError(invited_user_pk))?,
        )
    } else {
        None
    };
    let user_group_invite_detailed = run_serializable_transaction(&mut connection, |connection| {
        async move {
            let UserGroupJoined { group, membership } =
                get_user_group_joined(user_group_pk, Some(&user), connection).await?;
            if !(group.owner.pk == user.pk
                || (group.allow_member_invite
                    && membership
                        .as_ref()
                        .is_some_and(|membership| !membership.revoked))
                || is_user_group_detailed_editable(&group, &user, membership.as_ref()))
            {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InaccessibleObjectError(user_group_pk),
                ));
            }
            if let Some(invited_user_pk) = request.invited_user_pk {
                if invited_user_pk == group.owner.pk {
                    return Err(TransactionRuntimeError::Rollback(
                        Error::UserAlreadyMemberOfGroupError(group.pk),
                    ));
                }

                let existing_membership = user_group_membership::table
                    .filter(
                        user_group_membership::fk_user
                            .eq(invited_user_pk)
                            .and(user_group_membership::fk_group.eq(group.pk)),
                    )
                    .get_result::<UserGroupMembership>(connection)
                    .await
                    .optional()?;
                if let Some(existing_membership) = existing_membership {
                    if existing_membership.revoked {
                        return Err(TransactionRuntimeError::Rollback(
                            Error::UserBannedFromGroupError(group.pk),
                        ));
                    } else {
                        return Err(TransactionRuntimeError::Rollback(
                            Error::UserAlreadyMemberOfGroupError(group.pk),
                        ));
                    }
                }
            }

            let code = generate_user_group_invite_code();

            let expiration_timestamp = if let Some(expiration_days) = request.expiration_days {
                Some(
                    Utc::now()
                        .checked_add_days(Days::new(expiration_days as u64))
                        .ok_or_else(|| {
                            Error::InternalError(String::from(
                                "failed to add days to expiration date",
                            ))
                        })?,
                )
            } else {
                None
            };

            let now = Utc::now();
            diesel::insert_into(user_group_audit_log::table)
                .values(NewUserGroupAuditLog {
                    fk_user_group: group.pk,
                    fk_user: user.pk,
                    action: UserGroupAuditAction::Invite,
                    fk_target_user: invited_user.as_ref().map(|u| u.pk),
                    invite_code: Some(code.clone()),
                    reason: None,
                    creation_timestamp: now,
                })
                .execute(connection)
                .await?;

            let user_group_invite = diesel::insert_into(user_group_invite::table)
                .values(UserGroupInvite {
                    code,
                    fk_user_group: group.pk,
                    fk_create_user: user.pk,
                    fk_invited_user: invited_user.as_ref().map(|u| u.pk),
                    creation_timestamp: now,
                    expiration_timestamp,
                    last_used_timestamp: None,
                    max_uses: request.max_uses,
                    uses_count: 0,
                    revoked: false,
                })
                .get_result::<UserGroupInvite>(connection)
                .await
                .map_err(retry_on_constraint_violation)?;

            let (create_user, invited_user) =
                diesel::alias!(registered_user as create_user, registered_user as edit_user);
            let (create_user, invited_user) = user_group_invite::table
                .inner_join(create_user.on(
                    user_group_invite::fk_create_user.eq(create_user.field(registered_user::pk)),
                ))
                .left_join(
                    invited_user.on(user_group_invite::fk_invited_user
                        .eq(invited_user.field(registered_user::pk).nullable())),
                )
                .select((
                    create_user.fields(registered_user::table::all_columns()),
                    invited_user
                        .fields(registered_user::table::all_columns())
                        .nullable(),
                ))
                .filter(user_group_invite::code.eq(&user_group_invite.code))
                .get_result::<(UserPublic, Option<UserPublic>)>(connection)
                .await
                .map_err(Error::from)?;

            Ok(UserGroupInviteDetailed {
                code: user_group_invite.code,
                user_group: group,
                create_user,
                invited_user,
                creation_timestamp: user_group_invite.creation_timestamp,
                expiration_timestamp: user_group_invite.expiration_timestamp,
                last_used_timestamp: user_group_invite.last_used_timestamp,
                max_uses: user_group_invite.max_uses,
                uses_count: user_group_invite.uses_count,
                revoked: user_group_invite.revoked,
            })
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(&user_group_invite_detailed))
}

fn generate_user_group_invite_code() -> String {
    let mut rng = rand::rng();
    Alphanumeric.sample_string(&mut rng, 10)
}

pub async fn redeem_user_group_invite_handler(
    invite_code: String,
    user: User,
) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;

    let user_group_joined = run_serializable_transaction(&mut connection, |connection| {
        async move {
            let now = Utc::now();
            let invite = user_group_invite::table
                .filter(
                    user_group_invite::code
                        .eq(&invite_code)
                        .and(
                            user_group_invite::fk_invited_user
                                .is_null()
                                .or(user_group_invite::fk_invited_user.eq(user.pk)),
                        )
                        .and(
                            user_group_invite::expiration_timestamp
                                .is_null()
                                .or(user_group_invite::expiration_timestamp.gt(&now)),
                        )
                        .and(
                            user_group_invite::max_uses
                                .is_null()
                                .or(user_group_invite::max_uses
                                    .gt(user_group_invite::uses_count.nullable())),
                        )
                        .and(user_group_invite::revoked.eq(false)),
                )
                .get_result::<UserGroupInvite>(connection)
                .await
                .optional()?
                .ok_or_else(|| Error::InvalidUserGroupInviteCodeError(invite_code.clone()))?;

            let (group, membership) = user_group::table
                .left_join(
                    user_group_membership::table.on(user_group_membership::fk_group
                        .eq(user_group::pk)
                        .and(user_group_membership::fk_user.eq(user.pk))),
                )
                .filter(user_group::pk.eq(invite.fk_user_group))
                .get_result::<(UserGroup, Option<UserGroupMembership>)>(connection)
                .await?;

            if group.fk_owner == user.pk {
                return Err(TransactionRuntimeError::Rollback(
                    Error::UserAlreadyMemberOfGroupError(invite.fk_user_group),
                ));
            }

            if let Some(membership) = membership {
                if membership.revoked {
                    return Err(TransactionRuntimeError::Rollback(
                        Error::UserBannedFromGroupError(invite.fk_user_group),
                    ));
                } else {
                    return Err(TransactionRuntimeError::Rollback(
                        Error::UserAlreadyMemberOfGroupError(invite.fk_user_group),
                    ));
                }
            }

            diesel::insert_into(user_group_membership::table)
                .values(UserGroupMembership {
                    fk_group: invite.fk_user_group,
                    fk_user: user.pk,
                    administrator: false,
                    revoked: false,
                    fk_granted_by: invite.fk_create_user,
                    creation_timestamp: now,
                })
                .get_result::<UserGroupMembership>(connection)
                .await?;

            diesel::update(user_group_invite::table)
                .filter(user_group_invite::code.eq(&invite_code))
                .set((
                    user_group_invite::last_used_timestamp.eq(&now),
                    user_group_invite::uses_count.eq(user_group_invite::uses_count + 1),
                ))
                .execute(connection)
                .await?;

            // if the invite is a targeted invite for a specific user, delete after use (including duplicate invites for same group)
            diesel::delete(user_group_invite::table)
                .filter(
                    user_group_invite::fk_user_group
                        .eq(invite.fk_user_group)
                        .and(user_group_invite::fk_invited_user.eq(user.pk)),
                )
                .execute(connection)
                .await?;

            diesel::insert_into(user_group_audit_log::table)
                .values(NewUserGroupAuditLog {
                    fk_user_group: invite.fk_user_group,
                    fk_user: user.pk,
                    action: UserGroupAuditAction::Join,
                    fk_target_user: None,
                    invite_code: Some(invite_code.clone()),
                    reason: None,
                    creation_timestamp: now,
                })
                .execute(connection)
                .await?;

            let user_group_joined =
                get_user_group_joined(invite.fk_user_group, Some(&user), connection).await?;
            Ok(user_group_joined)
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(&user_group_joined))
}

pub async fn join_user_group_handler(
    user_group_pk: i64,
    user: User,
) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;
    let user_group_joined = run_serializable_transaction(&mut connection, |connection| {
        async {
            let membership = diesel::alias!(user_group_membership as membership,);
            let (group, membership) = user_group::table
                .left_join(
                    membership.on(membership
                        .field(user_group_membership::fk_group)
                        .eq(user_group::pk)
                        .and(membership.field(user_group_membership::fk_user).eq(user.pk))),
                )
                .filter(
                    user_group::pk.eq(user_group_pk).and(
                        user.is_admin
                            .into_sql::<Bool>()
                            .or(user_group::public)
                            .or(get_group_membership_condition!(user.pk)),
                    ),
                )
                .get_result::<(UserGroup, Option<UserGroupMembership>)>(connection)
                .await?;

            if group.fk_owner == user.pk {
                return Err(TransactionRuntimeError::Rollback(
                    Error::UserAlreadyMemberOfGroupError(user_group_pk),
                ));
            }

            if let Some(membership) = membership {
                if membership.revoked {
                    return Err(TransactionRuntimeError::Rollback(
                        Error::UserBannedFromGroupError(user_group_pk),
                    ));
                } else {
                    return Err(TransactionRuntimeError::Rollback(
                        Error::UserAlreadyMemberOfGroupError(user_group_pk),
                    ));
                }
            }

            let now = Utc::now();
            diesel::insert_into(user_group_membership::table)
                .values(UserGroupMembership {
                    fk_group: group.pk,
                    fk_user: user.pk,
                    administrator: false,
                    revoked: false,
                    fk_granted_by: user.pk,
                    creation_timestamp: now,
                })
                .get_result::<UserGroupMembership>(connection)
                .await?;

            diesel::insert_into(user_group_audit_log::table)
                .values(NewUserGroupAuditLog {
                    fk_user_group: group.pk,
                    fk_user: user.pk,
                    action: UserGroupAuditAction::Join,
                    fk_target_user: None,
                    invite_code: None,
                    reason: None,
                    creation_timestamp: now,
                })
                .execute(connection)
                .await?;

            let user_group_joined =
                get_user_group_joined(group.pk, Some(&user), connection).await?;
            Ok(user_group_joined)
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(&user_group_joined))
}

#[derive(Deserialize, Validate)]
pub struct GetUserGroupInvitesParams {
    #[validate(range(min = 1, max = 50))]
    pub limit: Option<u32>,
    #[validate(range(min = 0, max = 1000))]
    pub page: Option<u32>,
    pub ordering: Option<String>,
    pub active_only: Option<bool>,
}

#[derive(Serialize)]
pub struct UserGroupInviteInnerJoined {
    pub code: String,
    pub create_user: UserPublic,
    pub invited_user: Option<UserPublic>,
    pub creation_timestamp: DateTime<Utc>,
    pub expiration_timestamp: Option<DateTime<Utc>>,
    pub last_used_timestamp: Option<DateTime<Utc>>,
    pub max_uses: Option<i32>,
    pub uses_count: i32,
    pub revoked: bool,
    pub active: bool,
}

#[derive(Serialize)]
pub struct GetUserGroupInvitesResponse {
    pub total_count: i64,
    pub invites: Vec<UserGroupInviteInnerJoined>,
}

pub async fn get_user_group_invites_handler(
    user_group_pk: i64,
    params: GetUserGroupInvitesParams,
    user: User,
) -> Result<impl Reply, Rejection> {
    params.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for GetUserGroupInvitesParams: {e}"
        )))
    })?;
    let active_only = params.active_only.unwrap_or(false);

    let mut connection = acquire_db_connection().await?;
    let response = run_repeatable_read_transaction(&mut connection, |connection| {
        async {
            // only allow group admins / owner to view invites
            if !perms::is_user_group_editable(user_group_pk, Some(&user), connection).await? {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InaccessibleObjectError(user_group_pk),
                ));
            }

            let now = Utc::now();
            let filter = user_group_invite::fk_user_group.eq(user_group_pk).and(
                not(active_only.into_sql::<Bool>()).or(not(user_group_invite::revoked)
                    .and(
                        user_group_invite::expiration_timestamp
                            .is_null()
                            .or(user_group_invite::expiration_timestamp.gt(&now)),
                    )
                    .and(user_group_invite::max_uses.is_null().or(
                        user_group_invite::max_uses.gt(user_group_invite::uses_count.nullable()),
                    ))),
            );

            let total_count = user_group_invite::table
                .filter(filter)
                .count()
                .get_result::<i64>(connection)
                .await?;

            let (create_user, invited_user) = diesel::alias!(
                registered_user as create_user,
                registered_user as invited_user,
            );

            let invite_query = user_group_invite::table
                .inner_join(
                    create_user.on(create_user
                        .field(registered_user::pk)
                        .eq(user_group_invite::fk_create_user)),
                )
                .left_join(
                    invited_user.on(invited_user
                        .field(registered_user::pk)
                        .nullable()
                        .eq(user_group_invite::fk_invited_user)),
                )
                .filter(filter);

            let mut order_map: HashMap<&'static str, Box<ApplyOrderFn<_, _, _>>> = HashMap::new();
            order_map.insert(
                "create_user",
                Box::new(move |desc, q: BoxedSelectStatement<'_, _, _, _>| {
                    if desc {
                        q.order((
                            create_user.field(registered_user::pk).desc(),
                            user_group_invite::creation_timestamp.desc(),
                            user_group_invite::code,
                        ))
                    } else {
                        q.order((
                            create_user.field(registered_user::pk),
                            user_group_invite::creation_timestamp.desc(),
                            user_group_invite::code,
                        ))
                    }
                }),
            );
            order_map.insert(
                "creation_timestamp",
                order_by_col_with_tie_fn(
                    user_group_invite::creation_timestamp,
                    user_group_invite::code,
                ),
            );
            order_map.insert(
                "expiration_timestamp",
                order_by_col_with_tie_fn(
                    user_group_invite::expiration_timestamp,
                    user_group_invite::code,
                ),
            );
            order_map.insert(
                "last_used_timestamp",
                order_by_col_with_tie_fn(
                    user_group_invite::last_used_timestamp,
                    user_group_invite::code,
                ),
            );
            order_map.insert("code", order_by_col_fn(user_group_invite::code));

            let invite_query_ordered = apply_key_ordering(
                params.ordering,
                ("creation_timestamp", true),
                invite_query.into_boxed(),
                order_map,
            )?;

            let limit = params.limit.unwrap_or(50);
            let records = invite_query_ordered
                .limit(limit as i64)
                .offset((params.page.unwrap_or(0) * limit) as i64)
                .load::<(UserGroupInvite, UserPublic, Option<UserPublic>)>(connection)
                .await?;

            let invites = records
                .into_iter()
                .map(
                    |(invite, create_user, invited_user)| UserGroupInviteInnerJoined {
                        code: invite.code,
                        create_user,
                        invited_user,
                        creation_timestamp: invite.creation_timestamp,
                        expiration_timestamp: invite.expiration_timestamp,
                        last_used_timestamp: invite.last_used_timestamp,
                        max_uses: invite.max_uses,
                        uses_count: invite.uses_count,
                        revoked: invite.revoked,
                        active: !invite.revoked
                            && invite
                                .expiration_timestamp
                                .map(|exp| exp > now)
                                .unwrap_or(true)
                            && invite
                                .max_uses
                                .map(|max_uses| max_uses > invite.uses_count)
                                .unwrap_or(true),
                    },
                )
                .collect::<Vec<_>>();

            Ok(GetUserGroupInvitesResponse {
                total_count,
                invites,
            })
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(&response))
}

#[derive(Deserialize, Validate)]
pub struct GetCurrentUserGroupInvitesParams {
    #[validate(range(min = 1, max = 50))]
    pub limit: Option<u32>,
    #[validate(range(min = 0, max = 1000))]
    pub page: Option<u32>,
    pub ordering: Option<String>,
}

#[derive(Serialize)]
pub struct GetCurrentUserGroupInvitesResponse {
    pub total_count: i64,
    pub invites: Vec<UserGroupInviteDetailed>,
}

pub async fn get_current_user_group_invites_handler(
    params: GetCurrentUserGroupInvitesParams,
    user: User,
) -> Result<impl Reply, Rejection> {
    params.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for GetCurrentUserGroupInvitesParams: {e}"
        )))
    })?;

    let mut connection = acquire_db_connection().await?;
    let response = run_repeatable_read_transaction(&mut connection, |connection| {
        async {
            let now = Utc::now();
            let filter = user_group_invite::fk_invited_user.eq(user.pk).and(
                not(user_group_invite::revoked)
                    .and(
                        user_group_invite::expiration_timestamp
                            .is_null()
                            .or(user_group_invite::expiration_timestamp.gt(&now)),
                    )
                    .and(user_group_invite::max_uses.is_null().or(
                        user_group_invite::max_uses.gt(user_group_invite::uses_count.nullable()),
                    )),
            );

            let total_count = user_group_invite::table
                .filter(filter)
                .count()
                .get_result::<i64>(connection)
                .await?;

            let (
                invite_create_user,
                invited_user,
                group,
                group_create_user,
                group_edit_user,
                group_owner,
            ) = diesel::alias!(
                registered_user as invite_create_user,
                registered_user as invited_user,
                user_group as group,
                registered_user as group_create_user,
                registered_user as group_edit_user,
                registered_user as group_owner,
            );

            let invite_query = user_group_invite::table
                .inner_join(
                    invite_create_user.on(invite_create_user
                        .field(registered_user::pk)
                        .eq(user_group_invite::fk_create_user)),
                )
                .left_join(
                    invited_user.on(invited_user
                        .field(registered_user::pk)
                        .nullable()
                        .eq(user_group_invite::fk_invited_user)),
                )
                .inner_join(
                    group.on(group
                        .field(user_group::pk)
                        .eq(user_group_invite::fk_user_group)),
                )
                .inner_join(
                    group_create_user.on(group_create_user
                        .field(registered_user::pk)
                        .eq(group.field(user_group::fk_create_user))),
                )
                .inner_join(
                    group_edit_user.on(group_edit_user
                        .field(registered_user::pk)
                        .eq(group.field(user_group::fk_edit_user))),
                )
                .inner_join(
                    group_owner.on(group_owner
                        .field(registered_user::pk)
                        .eq(group.field(user_group::fk_owner))),
                )
                .filter(filter);

            let mut order_map: HashMap<&'static str, Box<ApplyOrderFn<_, _, _>>> = HashMap::new();
            order_map.insert(
                "group.name",
                Box::new(move |desc, q: BoxedSelectStatement<'_, _, _, _>| {
                    if desc {
                        q.order((
                            group.field(user_group::name).desc(),
                            user_group_invite::creation_timestamp.desc(),
                            user_group_invite::code,
                        ))
                    } else {
                        q.order((
                            group.field(user_group::name),
                            user_group_invite::creation_timestamp.desc(),
                            user_group_invite::code,
                        ))
                    }
                }),
            );
            order_map.insert(
                "creation_timestamp",
                order_by_col_with_tie_fn(
                    user_group_invite::creation_timestamp,
                    user_group_invite::code,
                ),
            );
            order_map.insert(
                "expiration_timestamp",
                order_by_col_with_tie_fn(
                    user_group_invite::expiration_timestamp,
                    user_group_invite::code,
                ),
            );
            order_map.insert(
                "last_used_timestamp",
                order_by_col_with_tie_fn(
                    user_group_invite::last_used_timestamp,
                    user_group_invite::code,
                ),
            );
            order_map.insert("code", order_by_col_fn(user_group_invite::code));

            let invite_query_ordered = apply_key_ordering(
                params.ordering,
                ("creation_timestamp", true),
                invite_query.into_boxed(),
                order_map,
            )?;

            let limit = params.limit.unwrap_or(50);
            let records = invite_query_ordered
                .limit(limit as i64)
                .offset((params.page.unwrap_or(0) * limit) as i64)
                .load::<(
                    UserGroupInvite,
                    UserPublic,
                    Option<UserPublic>,
                    UserGroup,
                    UserPublic,
                    UserPublic,
                    UserPublic,
                )>(connection)
                .await?;

            let invites = records
                .into_iter()
                .map(
                    |(
                        invite,
                        invite_create_user,
                        invited_user,
                        group,
                        group_create_user,
                        group_edit_user,
                        group_owner,
                    )| UserGroupInviteDetailed {
                        code: invite.code,
                        user_group: UserGroupDetailed {
                            pk: group.pk,
                            name: group.name,
                            public: group.public,
                            owner: group_owner,
                            creation_timestamp: group.creation_timestamp,
                            description: group.description,
                            allow_member_invite: group.allow_member_invite,
                            avatar_object_key: group.avatar_object_key,
                            create_user: group_create_user,
                            edit_timestamp: group.edit_timestamp,
                            edit_user: group_edit_user,
                            tags: None,
                            is_editable: false,
                            user_can_invite: false,
                        },
                        create_user: invite_create_user,
                        invited_user,
                        creation_timestamp: invite.creation_timestamp,
                        expiration_timestamp: invite.expiration_timestamp,
                        last_used_timestamp: invite.last_used_timestamp,
                        max_uses: invite.max_uses,
                        uses_count: invite.uses_count,
                        revoked: invite.revoked,
                    },
                )
                .collect::<Vec<_>>();

            Ok(GetCurrentUserGroupInvitesResponse {
                total_count,
                invites,
            })
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(&response))
}

#[derive(Serialize)]
pub struct RevokeUserGroupInviteResponse {
    prev: UserGroupInvite,
    updated: Option<UserGroupInvite>,
}

pub async fn revoke_user_group_invite_handler(
    invite_code: String,
    user: User,
) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;
    let response = run_serializable_transaction(&mut connection, |connection| {
        async {
            let invite = user_group_invite::table
                .filter(user_group_invite::code.eq(&invite_code))
                .get_result::<UserGroupInvite>(connection)
                .await
                .optional()?
                .ok_or_else(|| {
                    TransactionRuntimeError::Rollback(Error::InaccessibleObjectKeyError(
                        invite_code.clone(),
                    ))
                })?;

            let (group, membership) = user_group::table
                .left_join(
                    user_group_membership::table.on(user_group_membership::fk_group
                        .eq(user_group::pk)
                        .and(user_group_membership::fk_user.eq(user.pk))),
                )
                .filter(user_group::pk.eq(invite.fk_user_group))
                .get_result::<(UserGroup, Option<UserGroupMembership>)>(connection)
                .await?;

            if !(is_user_group_editable(&group, &user, membership.as_ref())
                || invite.fk_create_user == user.pk
                || invite.fk_invited_user == Some(user.pk))
            {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InaccessibleObjectKeyError(invite_code),
                ));
            }

            if invite.revoked {
                return Ok(RevokeUserGroupInviteResponse {
                    prev: invite,
                    updated: None,
                });
            }

            let updated = diesel::update(user_group_invite::table)
                .filter(user_group_invite::code.eq(&invite_code))
                .set(user_group_invite::revoked.eq(true))
                .get_result::<UserGroupInvite>(connection)
                .await?;

            diesel::insert_into(user_group_audit_log::table)
                .values(NewUserGroupAuditLog {
                    fk_user_group: invite.fk_user_group,
                    fk_user: user.pk,
                    action: UserGroupAuditAction::RevokeInvite,
                    fk_target_user: invite.fk_invited_user,
                    invite_code: Some(invite_code),
                    reason: None,
                    creation_timestamp: Utc::now(),
                })
                .execute(connection)
                .await?;

            Ok(RevokeUserGroupInviteResponse {
                prev: invite,
                updated: Some(updated),
            })
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(&response))
}
